# coding: utf-8
# In[519]:
import matplotlib.pyplot as plt
import urllib2
import psycopg2
import ast
from Config import redshift
import datetime
import numpy as np
# In[521]:
sending_table = np.genfromtxt('silver_24hours.txt', delimiter=',')
# In[522]:
send_period_start = 0
def_period_start = 0
sending_result = np.empty((0,3), int)
num_send = 0
num_def = 0
for i in range(len(sending_table)-1):
    if sending_table[i][3] == 1.0 and sending_table[i+1][3] != 3.0:
        num_send +=1
    if sending_table[i][3] == 1.0 and sending_table[i+1][3] == 3.0:
        num_send +=1
        send_period_end = i+1
        sending_result = np.vstack((sending_result,[sending_table[send_period_end][1]-sending_table[send_period_start][1],num_send,1]))
        num_send = 0
        def_period_start = i+1
    if sending_table[i][3] == 3 and sending_table[i+1][3] != 1.0:
        num_def +=1 
    if sending_table[i][3] == 3 and sending_table[i+1][3] == 1.0:
        num_def +=1
        def_period_end = i+1
        sending_result = np.vstack((sending_result,[sending_table[def_period_end][1]-sending_table[def_period_start][1],num_def,3]))
        num_def = 0
        send_period_start = i+1
# In[547]:
fig, ax = plt.subplots()
ax2 = ax.twiny()
rects = np.empty((0,np.shape(sending_result)[0]), int)
x = [sending_table[0][1]]
x_h = [0]
# In[548]:
for i in range(np.shape(sending_result)[0]-1):
    x = np.hstack((x,x[i]+sending_result[i][0]))
h = 3600

var = max(x)
#while h is less than the cumulative time 
while h <  var: ####Bug to be fixed ####
    x_h = np.hstack((x_h,h))
    h += 3600
# In[549]:
x_ah = x_h/3600+sending_table[0][4] ### BUG to be fixed ###
# In[550]:
while True in (x_ah>23):
    for i in range(len(x_ah)):
        if x_ah[i] > 23:
            x_ah[i] = x_ah[i] - 24
x_ah = x_ah.astype(int)
# In[551]:
def autolabel(rects):
    # attach speed labels#
    for rect in rects:
        ax.text(rect.get_x()+rect.get_width()/2., 1.05*rect.get_height(), '1/%d' %int(rect.get_width()/rect.get_height()),ha='center', va='bottom')
i = 0
while i < np.shape(sending_result)[0]:
    if sending_result[i][2] == 1:
        rects1 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='b')
        autolabel(rects1)
        if i+1 < (np.shape(sending_result)[0]-1):
            rects2 = ax.bar(x[i+1], sending_result[i+1][1], width=sending_result[i+1][0],color='r')
            autolabel(rects2)
    else:
        rects1 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='r')
        autolabel(rects1)
        if i+1 < (np.shape(sending_result)[0]-1):
            rects2 = ax.bar(x[i+1], sending_result[i+1][1], width=sending_result[i+1][0],color='b')
            autolabel(rects2)
    i += 2
ax.legend( (rects1[0], rects2[0]), (len(sending_table[sending_table[:,3]==1]), len(sending_table[sending_table[:,3]!=1])) )
ax.set_title('silverroller.com 24 hour sends',y=1.08) #change title#
ax.set_xlabel(r"seconds")
ax.set_ylim(0,400)
timerange = 86400 
ax.set_xlim(0,timerange) ## Bug to be fixed ###########
ax2.set_xticks(x_h)
ax2.set_xticklabels(x_ah)
ax2.set_xlabel(r"time (in 24-hour)")
# In[552]:
plt.show()
