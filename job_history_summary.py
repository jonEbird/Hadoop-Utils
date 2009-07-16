#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
import sys

pat = re.compile('(?P<name>[^=]+)="(?P<value>[^"]*)" *')
groupPat = re.compile(r'{\((?P<key>[^)]+)\)\((?P<name>[^)]+)\)(?P<counters>[^}]+)}')
counterPat = re.compile(r'\[\((?P<key>[^)]+)\)\((?P<name>[^)]+)\)\((?P<value>[^)]+)\)\]')

def parseCounters(str):
  result = {}
  for k,n,c in re.findall(groupPat, str):
    group = {}
    result[n] = group
    for sk, sn, sv in re.findall(counterPat, c):
      group[sn] = int(sv)
  return result

def parse(tail):
  result = {}
  for n,v in re.findall(pat, tail):
    result[n] = v
  return result

mapStartTime = {}
mapEndTime = {}
reduceStartTime = {}
reduceShuffleTime = {}
reduceSortTime = {}
reduceEndTime = {}
reduceBytes = {}
remainder = ""
finalAttempt = {}
wastedAttempts = []
subitTime = None
finishTime = None
scale = 1000

for line in sys.stdin:
  if len(line) < 3 or line[-3:] != " .\n":
    remainder += line
    continue
  line = remainder + line
  remainder = ""
  words = line.split(" ",1)
  event = words[0]
  attrs = parse(words[1])
  if event == 'Job':
    if attrs.has_key("SUBMIT_TIME"):
      submitTime = int(attrs["SUBMIT_TIME"]) / scale
    elif attrs.has_key("FINISH_TIME"):
      finishTime = int(attrs["FINISH_TIME"]) / scale
  elif event == 'MapAttempt':
    if attrs.has_key("START_TIME"):
      time = int(attrs["START_TIME"]) / scale
      if time != 0:
        mapStartTime[attrs["TASK_ATTEMPT_ID"]] = time
    elif attrs.has_key("FINISH_TIME"):
      mapEndTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["FINISH_TIME"])/scale
      if attrs.get("TASK_STATUS", "") == "SUCCESS":
        task = attrs["TASKID"]
        if finalAttempt.has_key(task):
          wastedAttempts.append(finalAttempt[task])
        finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
      else:
        wastedAttempts.append(attrs["TASK_ATTEMPT_ID"])
  elif event == 'ReduceAttempt':
    if attrs.has_key("START_TIME"):
      time = int(attrs["START_TIME"]) / scale
      if time != 0:
        reduceStartTime[attrs["TASK_ATTEMPT_ID"]] = time
    elif attrs.has_key("FINISH_TIME"):
      task = attrs["TASKID"]
      if attrs.get("TASK_STATUS", "") == "SUCCESS":
        if finalAttempt.has_key(task):
          wastedAttempts.append(finalAttempt[task])
        finalAttempt[task] = attrs["TASK_ATTEMPT_ID"]
      else:
        wastedAttempts.append(attrs["TASK_ATTEMPT_ID"])
      reduceEndTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["FINISH_TIME"]) / scale
      if attrs.has_key("SHUFFLE_FINISHED"):
        reduceShuffleTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["SHUFFLE_FINISHED"]) / scale
      if attrs.has_key("SORT_FINISHED"):
        reduceSortTime[attrs["TASK_ATTEMPT_ID"]] = int(attrs["SORT_FINISHED"]) / scale
  elif event == 'Task':
    if attrs["TASK_TYPE"] == "REDUCE" and attrs.has_key("COUNTERS"):
      counters = parseCounters(attrs["COUNTERS"])
      reduceBytes[attrs["TASKID"]] = int(counters.get('FileSystemCounters',{}).get('HDFS_BYTES_WRITTEN',0))

reduces = reduceBytes.keys()
reduces.sort()

print "Name reduce-output-bytes shuffle-finish reduce-finish"
for r in reduces:
  attempt = finalAttempt[r]
  print r, reduceBytes[r], reduceShuffleTime[attempt] - submitTime,
  print reduceEndTime[attempt] - submitTime

print

runningMaps = []
shufflingReduces = []
sortingReduces = []
runningReduces = []

waste = []
final = {}
for t in finalAttempt.values():
  final[t] = None

for t in range(submitTime, finishTime):
  runningMaps.append(0)
  shufflingReduces.append(0)
  sortingReduces.append(0)
  runningReduces.append(0)
  waste.append(0)

for map in mapEndTime.keys():
  isFinal = final.has_key(map)
  if mapStartTime.has_key(map):
    for t in range(mapStartTime[map]-submitTime, mapEndTime[map]-submitTime):
      if final:
        runningMaps[t] += 1
      else:
        waste[t] += 1

for reduce in reduceEndTime.keys():
  if reduceStartTime.has_key(reduce):
    if final.has_key(reduce):
      for t in range(reduceStartTime[reduce]-submitTime, reduceShuffleTime[reduce]-submitTime):
        shufflingReduces[t] += 1
      for t in range(reduceShuffleTime[reduce]-submitTime, reduceSortTime[reduce]-submitTime):
        sortingReduces[t] += 1
      for t in range(reduceSortTime[reduce]-submitTime, reduceEndTime[reduce]-submitTime):
        runningReduces[t] += 1
    else:
      for t in range(reduceStartTime[reduce]-submitTime, reduceEndTime[reduce]-submitTime):
        waste[t] += 1

print "time maps shuffle merge reduce waste"
for t in range(len(runningMaps)):
  print t, runningMaps[t], shufflingReduces[t], sortingReduces[t], runningReduces[t], waste[t]
