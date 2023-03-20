from flask import Flask, render_template, request, redirect, url_for, send_from_directory


app = Flask(__name__)

# Este controla la pagina inicial de nuestra Web App
@app.route('/')
def index():
   return "¡La app está activa!"

# Iniciamos nuestra app
if __name__ == '__main__':
   app.run()

