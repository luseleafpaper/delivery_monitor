import matplotlib.pyplot as plt
import urllib2
import psycopg2
import ast
from Connection import Connection
import datetime
from datetime import timedelta
import numpy as np

def main(): 
	print("In main")
	con = Connection("piston2")
	con.speak()
	plot = Plot()
	plot.speak()
	campaigns = ['amber%', 'silver%', 'tele%'] 
	for c in campaigns: 
		plot.set_campaign(c)
		plot.set_lookback_duration(48)
		plot.set_end_point(20)
		plot.fetch_data(con)
		plot.generate_plot()
	
class Plot: 
	def __init__(self):
		pass
		
	def speak(self): 
		print("I'm a Plot object")
	
	def set_end_point(self, rollback=0): 
		self.end_point = datetime.datetime.now() - timedelta(hours=rollback)
	
	def set_lookback_duration(self, duration=24):
		self.duration = duration
		
	def get_range(self): 
		start_point = self.end_point - timedelta(hours=self.duration)
		end_point = self.end_point
		return start_point, end_point
	
	def get_campaign(self): 
		return self.campaign
	
	def set_campaign(self, campaign):
		self.campaign = campaign

	def fetch_data(self, connection): 
		start_point, end_point = self.get_range()
		campaign_name = self.get_campaign()
		query = """
			select
			campaign,
			EXTRACT(EPOCH FROM ((send_ts-interval '4 hours')- to_timestamp('%s', 'YYYY-MM-DD HH24-MI-SS'))),
				(EXTRACT(EPOCH FROM ((send_ts-interval '4 hours')- to_timestamp('%s', 'YYYY-MM-DD HH24-MI-SS')))/3600)::int,
				send_status,
			DATE_PART('hour',send_ts)-4 as hour,
			DATE_PART('minute',send_ts) as minute   
			from sends
			WHERE (send_ts-interval '4 hours') > to_timestamp('%s', 'YYYY-MM-DD HH24-MI-SS') and 
			 (send_ts-interval '4 hours') < to_timestamp('%s', 'YYYY-MM-DD HH24-MI-SS') 
			and send_status in (1, 3)
			and (campaign like '%s' )
			order by 2;
			""" %(start_point,start_point,start_point, end_point, campaign_name)
		data = connection.fetchall(query)
		self.data = data 
		return data 

	def generate_plot(self, data=None): 
		sending_table = self.data 
		start_point, end_point = self.get_range()
		campaign_name = self.get_campaign()

		# In[10]:

		send_period_start = 0
		def_period_start = 0
		sending_result = np.empty((0,3), int)
		num_send = 0
		num_def = 0
		for i in range(len(sending_table)-1):
			if (sending_table[i+1][1] - sending_table[i][1]) < 800:
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
			elif sending_table[i][3] == 1.0 and sending_table[i+1][3] == 3.0:
				num_send += 1
				send_period_end = i
				sending_result = np.vstack((sending_result,[sending_table[send_period_end][1]-sending_table[send_period_start][1],num_send,1]))
				num_send = 0
				def_period_start = i+1
				sending_result = np.vstack((sending_result,[sending_table[i+1][1] - sending_table[i][1],num_send,0]))
			elif sending_table[i][3] == 1.0 and sending_table[i+1][3] != 3.0:
				num_send += 1
				send_period_end = i
				sending_result = np.vstack((sending_result,[sending_table[send_period_end][1]-sending_table[send_period_start][1],num_send,1]))
				num_send = 0
				send_period_start = i+1
				sending_result = np.vstack((sending_result,[sending_table[i+1][1] - sending_table[i][1],num_send,0]))
			elif sending_table[i][3] == 3 and sending_table[i+1][3] != 1.0:
				num_def +=1
				def_period_end = i
				sending_result = np.vstack((sending_result,[sending_table[def_period_end][1]-sending_table[def_period_start][1],num_def,3]))
				num_def = 0
				def_period_start = i+1
				sending_result = np.vstack((sending_result,[sending_table[i+1][1] - sending_table[i][1],num_def,0]))
			elif sending_table[i][3] == 3 and sending_table[i+1][3] == 1.0:
				num_def +=1
				def_period_end = i
				sending_result = np.vstack((sending_result,[sending_table[def_period_end][1]-sending_table[def_period_start][1],num_def,3]))
				num_def = 0
				send_period_start = i+1
				sending_result = np.vstack((sending_result,[sending_table[i+1][1] - sending_table[i][1],num_def,0]))


		# In[60]:

		fig, ax = plt.subplots()
		ax2 = ax.twiny()
		rects = np.empty((0,np.shape(sending_result)[0]), int)
		x = [sending_table[0][1]]
		x_h = [0]


		# In[61]:

		for i in range(np.shape(sending_result)[0]-1):
			x = np.hstack((x,x[i]+sending_result[i][0]))
		h = 3600
		while h <= ((end_point-start_point).days)*86400+(end_point-start_point).seconds:
			x_h = np.hstack((x_h,h))
			h += 3600


		# In[62]:

		x_ah = x_h/3600+start_point.hour


		# In[63]:

		while True in (x_ah>23):
			for i in range(len(x_ah)):
				if x_ah[i] > 23:
					x_ah[i] = x_ah[i] - 24
		x_ah = x_ah.astype(int)


		# In[64]:

		def autolabel(rects):
			# attach speed labels#
			for rect in rects:
				ax.text(rect.get_x()+rect.get_width()/2., 1.05*rect.get_height(), '1/%d' %int(rect.get_width()/rect.get_height()),ha='center', va='bottom')
		i = 0
		while i < np.shape(sending_result)[0]:
			if sending_result[i][2] == 1:
				rects1 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='b')
				autolabel(rects1)
			if sending_result[i][2] == 3:
				rects2 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='r')
				autolabel(rects2)
				
			i += 1
		ax.legend((rects1[0], rects2[0]), (len(np.asarray(sending_table)[np.asarray(sending_table)[:,3]=='1']), len(np.asarray(sending_table)[np.asarray(sending_table)[:,3]=='3'])))
		ax.set_title('%s in %d day %d hour %d seconds plot from %s to %s' % (campaign_name,(end_point-start_point).days,(end_point-start_point).seconds/3600,(end_point-start_point).seconds%3600,datetime.datetime.strftime(start_point,"%Y-%m-%d %H:%M:%S"),datetime.datetime.strftime(end_point,"%Y-%m-%d %H:%M:%S")), y=1.08)#change title#
		ax.set_xlabel(r"seconds")
		ax.set_ylim(0,400)
		ax.set_xlim(0,(((end_point-start_point).days)*86400+(end_point-start_point).seconds)) 
		ax2.set_xticks(x_h)
		ax2.set_xticklabels(x_ah)
		ax2.set_xlabel(r"time (in 24-hour format)")


		# In[65]:

		fig.set_size_inches(28,12)


		# In[66]:

		#plt.show()
		plt.savefig('/media/sf_Shared_Folders/delivery_monitor/Static/images/%s.png' % self.campaign)
		


if __name__=="__main__": 
	main()