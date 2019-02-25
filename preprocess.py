# -*- coding: utf-8 -*-

import json
import os
import datetime
import heapq
input_list = ['gadgets']

threads = 'gadgets/'

if not os.path.exists(threads):
		os.makedirs(threads)

for file in input_list:
	print(file)
	dic = {}

	comments_file = open('gadgets_comm.txt')

	output_dir = threads + file 

	for line in comments_file:

		cur = json.loads(line)

		if 'id' not in cur:
			print('there is no id in this comments')
			print(line)

		if cur['link_id'] not in dic:
			array = []
			dic[cur['link_id']] = array
			cur['id'] = 't1_' + cur['id']

			if cur['distinguished'] == 'moderator':
				continue

			cur['body'] = cur['body'].strip().replace('\r\n',' ').replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').replace("[deleted]", ' ').replace('[removed]', ' ').strip()
			cur['body'] = ' '.join([s for s in cur['body'].split() if s != ''])

			# tokens = cur['body'].split()
			# if len(tokens) < 5:
			# 	continue		# first delete utterance less than 5 words or later?	

			(dic[cur['link_id']]).append(cur)

		else:

			cur['id'] = 't1_' + cur['id']

			if cur['distinguished'] == 'moderator':
				continue

			cur['body'] = cur['body'].strip().replace('\r\n',' ').replace('\r', ' ').replace('\n', ' ').replace('\t', ' ').replace("[deleted]", ' ').replace('[removed]', ' ').strip()
			cur['body'] = ' '.join([s for s in cur['body'].split() if s != ''])

			# tokens = cur['body'].split()
			# if len(tokens) < 5:
			# 	continue	# first delete utterance less than 5 words or later?
			
			(dic[cur['link_id']]).append(cur)

	time_map = []

	print("ori threads number: ", len(dic))

	for k, d in dic.items():
		
		# if len(d) < 10: #delete thread with less than 10 utterances, do it now or afterwards?
		# 	continue

		temp = [k, float("inf"), -1 * float("inf")]

		for v in d:

			temp[1] = min(temp[1], int(v['created_utc']))

			temp[2] = max(temp[2], int(v['created_utc']))

		time_map.append(temp)

	time_map = sorted(time_map, key=lambda x: x[1])
	#[id, start, end]
	heap = []

	for slot in time_map:#delete threads beyond the 10 threads at a time limit

		if heap and slot[1] > heap[0]:
			heapq.heapreplace(heap, slot[2])
		else:
			if len(heap) >= 10: 
				del dic[slot[0]]
			else:
				heapq.heappush(heap, slot[2])

		assert len(heap) <= 10
				

	print("after delete concurrent threads, threads number: ", len(dic))

	output_file = output_dir + '.txt'

	f = open(output_file, 'w')

	threads_count = 0

	message_count = 0

	for k, d in dic.items():
		lit = []
		for item in d:
			tokens = item['body'].split()
			if len(tokens) >= 5:
				lit.append(item)
		dic[k] = lit

	k_list = [] 
	for k, d in dic.items():

		if len(d) < 10:#if a thread has less than 10 utterance, exclude
			continue

		k_list.append(k)
		threads_count = threads_count + 1
		message_count = message_count + len(d)
		for v in d:
			temp = k

			assert v['subreddit'] == 'gadgets'

			temp = temp + '\t' + v['id']
			assert len(v['id']) != 0

			temp = temp + '\t' + str(v['created_utc'])

			assert len(str(v['created_utc'])) != 0

			if len(v['body']) == 0:
				print("&&&&&&",v)
				temp = temp + '\tNULL'
			else:
				temp = temp + '\t' + v['body']

			temp = temp + '\t' + v['author']
			assert len(v['author']) != 0

			temp = temp + '\t' + v['parent_id']
			assert len(v['parent_id']) != 0

			tokens = temp.split('\t')
			# assert len(tokens) == 6

			f.write(temp.encode('utf-8') + '\n')
			f.flush()
	

	print('final total threads: ', threads_count, '     total message: ', message_count)
	f.close()
	comments_file.close()