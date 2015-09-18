from flask import Flask, url_for
from flask import render_template
from Classes.Connection import Connection
from Classes.Plot import Plot
import os

app = Flask(__name__)
app.debug= True

@app.route('/')
def index(): 
	return "Hello World!"

@app.route('/login')
def login(): pass

@app.route('/user/<username>') 
def profile(username): pass 

@app.route('/images')
def images(type='sample'):
	
	img_dir = app.static_folder +'images/'
	print(img_dir)
	images = []

	for root, dirs, files in os.walk('/media/sf_Shared_Folders/delivery_monitor/Static/images', topdown=False):
		for name in files:
			print(os.path.join(img_dir, name))
			images.append('./static/images/'+name)
	
	rendered_html = render_template('test.html', images=images )
	return rendered_html
	
with app.test_request_context(): 
	print(url_for('index'))
	print(url_for('login', next='arg'))
	print(url_for('profile', username='John Doe'))

if __name__=='__main__': 
	app.run() 
