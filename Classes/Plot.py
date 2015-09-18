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
	plot = Plot(con)
	plot.speak()
	campaigns = ['amber%', 'silver%', 'tele%'] 
	for c in campaigns: 
		plot.set_campaign(c)
		plot.set_lookback_duration(24)
		plot.set_end_point(0)
		plot.generate_plot()
	
class Plot: 
	def __init__(self, connection):
		self.connection = connection
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
		
	def fetch_open_data(self): 
		start_point, end_point = self.get_range()
		campaign_name = self.get_campaign()
		query = """
		SELECT
			  send_ts::date,
			  DATE_PART('hour',send_ts),
			  sending_domain,
			  COUNT(s.message_token) AS sends,
			  COUNT(o.message_token) AS opens,
			  COUNT(DISTINCT o.message_token) / COUNT(DISTINCT s.message_token)::float *100 AS open_percent
		FROM sends s
		LEFT JOIN opens o ON o.message_token = s.message_token
		WHERE (send_ts-interval '4 hours') > to_timestamp('%s', 'YYYY-MM-DD HH24-MI-SS') and (send_ts-interval '4 hours') <= to_timestamp('%s', 'YYYY-MM-DD HH24-MI-SS') 
		and sending_domain like '%s'
		and send_status = 1
		GROUP BY 1
				,2
				,3
		ORDER BY 1
				,2
				,3 
		;
		""" %(start_point,end_point,campaign_name)
		print query
		open_table = self.connection.fetchall(query)
		
		open_table = np.asarray(open_table)

		open_table[0]

		for i in range(len(open_table)):
			if (open_table[i,1]-4) < 0:
				open_table[i,1] += 20
			else:
				open_table[i,1] += -4

		trans = datetime.datetime.combine(open_table[0][0],datetime.time(int(open_table[0][1]))).hour- start_point.hour


		# In[15]:

		for i in range(len(open_table)):
			open_table[i][1] = (i+trans)*3600
		return open_table
	
	def fetch_send_data(self): 
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
		print query
		data = self.connection.fetchall(query)
		return data 

	def generate_plot(self, data=None): 
		sending_table = self.fetch_send_data()
		open_table = self.fetch_open_data()
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
		# In[158]:
		fig1,ax = plt.subplots()
		ax2 = ax.twiny()
		rects = np.empty((0,np.shape(sending_result)[0]), int)
		x = [sending_table[0][1]]
		x_h = [start_point.minute*60]
		# In[159]:
		for i in range(np.shape(sending_result)[0]-1):
			x = np.hstack((x,x[i]+sending_result[i][0]))
		h = 3600
		while h< ((end_point-start_point).days)*86400+(end_point-start_point).seconds:
			x_h = np.hstack((x_h,h))
			h += 3600
		# In[160]:
		x_ah = x_h/3600+start_point.hour
		# In[161]:
		while True in (x_ah>23):
			for i in range(len(x_ah)):
				if x_ah[i] > 23:
					x_ah[i] = x_ah[i] - 24
		x_ah = x_ah.astype(int)
		# In[162]:
		def autolabel(rects):
			# attach speed labels#
			for rect in rects:
				ax.text(rect.get_x()+rect.get_width()/2., 1.05*rect.get_height(), '1/%d' %int(rect.get_width()/rect.get_height()),ha='center', va='bottom')
		i = 0
		xmin = 0
		xmax = (((end_point-start_point).days)*86400+(end_point-start_point).seconds)
		ymin = 0
		ymax = 400
		while i < np.shape(sending_result)[0]:
			if sending_result[i][2] == 1:
				rects1 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='b')
				autolabel(rects1)
			if sending_result[i][2] == 3:
				rects2 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='r')
				autolabel(rects2)
				##if i+1 < (np.shape(sending_result)[0]-1):
					#rects2 = ax.bar(x[i+1], sending_result[i+1][1], width=sending_result[i+1][0],color='r')
					#autolabel(rects2)
			#else:
				#rects1 = ax.bar(x[i],sending_result[i][1], width=sending_result[i][0], color='r')
				#autolabel(rects1)
				#if i+1 < (np.shape(sending_result)[0]-1):
					#rects2 = ax.bar(x[i+1], sending_result[i+1][1], width=sending_result[i+1][0],color='b')
					#autolabel(rects2)
			i += 1
		ax.legend((rects1[0], rects2[0]), (len(np.asarray(sending_table)[np.asarray(sending_table)[:,3]=='1']), len(np.asarray(sending_table)[np.asarray(sending_table)[:,3]=='3'])),bbox_to_anchor=(1.02, 1), loc=2, borderaxespad=0)
		ax.set_title('%s sends in %d day %d hour %d seconds plot from %s to %s' % (campaign_name,(end_point-start_point).days,(end_point-start_point).seconds/3600,(end_point-start_point).seconds%3600,datetime.datetime.strftime(start_point,"%Y-%m-%d %H:%M:%S"),datetime.datetime.strftime(end_point,"%Y-%m-%d %H:%M:%S")), y=1.05)#change title#
		ax.set_ylim(ymin,ymax)
		ax.set_ylabel(r"volume")
		ax.set_xlim(xmin,xmax) 
		ax2.xaxis.tick_top()
		ax2.yaxis.tick_right()
		ax2.axis([xmin, xmax, ymin, ymax])
		ax2.set_xticks(x_h)
		ax2.set_xticklabels(x_ah)
		ax2.set_xlabel(r"time (in 24-hour format)")
		fig1.set_size_inches(12*self.duration/24+2,12)
		plt.savefig('/media/sf_Shared_Folders/delivery_monitor/Static/images/sends-%s.png' % self.campaign)
		plt.close()
		# In[163]:
		fig2,axc = plt.subplots()
		xmin = 0
		xmax = (((end_point-start_point).days)*86400+(end_point-start_point).seconds)
		ymin = 0
		ymax = 5000
		ax3 = axc.twiny()
		axc.plot(x[sending_result[:,2]==1],sending_result[sending_result[:,2]==1][:,0],'b^:',label="sending")
		axc.plot(x[sending_result[:,2]==3],sending_result[sending_result[:,2]==3][:,0],'ro-',label="deferral")
		axc.legend(bbox_to_anchor=(1.02, 1), loc=2, borderaxespad=0)
		axc.set_title('%s duration in %d day %d hour %d seconds plot from %s to %s' % (campaign_name,(end_point-start_point).days,(end_point-start_point).seconds/3600,(end_point-start_point).seconds%3600,datetime.datetime.strftime(start_point,"%Y-%m-%d %H:%M:%S"),datetime.datetime.strftime(end_point,"%Y-%m-%d %H:%M:%S")), y=1.05)#change title#
		axc.set_ylim(ymin,ymax)
		axc.set_xlim(xmin,xmax)
		ax3.xaxis.tick_top()
		ax3.yaxis.tick_right()
		ax3.axis([xmin, xmax, ymin, ymax])
		ax3.set_xticks(x_h)
		ax3.set_xticklabels(x_ah)
		ax3.set_xlabel(r"time (in 24-hour format)")
		axc.set_ylabel(r"duration in seconds")
		fig2.set_size_inches(12*self.duration/24+2,12)
		plt.savefig('/media/sf_Shared_Folders/delivery_monitor/Static/images/duration-%s.png' % self.campaign)
		plt.close()
		# In[164]:
		fig3,axo = plt.subplots()
		xmin = 0
		xmax = (((end_point-start_point).days)*86400+(end_point-start_point).seconds)
		ymin = 0
		ymax = 7
		ax4 = axo.twiny()
		axo.plot(open_table[:,1],open_table[:,5],'g*--',label = "open rate")
		axo.set_xlabel(r"seconds")
		axo.set_ylabel(r"open rate")
		axo.legend(bbox_to_anchor=(1.02, 1), loc=2, borderaxespad=0)
		axo.set_title('%s opens in %d day %d hour %d seconds plot from %s to %s' % (campaign_name,(end_point-start_point).days,(end_point-start_point).seconds/3600,(end_point-start_point).seconds%3600,datetime.datetime.strftime(start_point,"%Y-%m-%d %H:%M:%S"),datetime.datetime.strftime(end_point,"%Y-%m-%d %H:%M:%S")), y=1.05)#change title#
		axo.set_ylim(ymin,ymax)
		axo.set_xlim(xmin,xmax)
		ax4.xaxis.tick_top()
		ax4.yaxis.tick_right()
		ax4.axis([xmin, xmax, ymin, ymax])
		ax4.set_xticks(x_h)
		ax4.set_xticklabels(x_ah)
		ax4.set_xlabel(r"time (in 24-hour format)")
		fig3.set_size_inches(12*self.duration/24+2,12)
		plt.savefig('/media/sf_Shared_Folders/delivery_monitor/Static/images/opens-%s.png' % self.campaign)
		plt.close()
		


if __name__=="__main__": 
	main()