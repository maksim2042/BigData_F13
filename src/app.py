from flask import Flask
app = Flask(__name__)

class RegistrationForm(Form):
    username     = TextField('Username')
    

@app.route("/", methods=["GET", "POST"])
def hello():
    pass
   # if request.method == 'GET':
   #     return "
    #    
    #    "
        
#    data=request.args
    
    
    
if __name__ == "__main__":
    app.run()