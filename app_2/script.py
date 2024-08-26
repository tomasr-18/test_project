from flask import Flask, request
import os

app2 = Flask(__name__)


@app2.route('/')
def hello():
    # Hämta värdet av URL-parametern 'name', använd 'World' som standard om inget anges
    name = request.args.get('name', 'World')
    return f'Hello, {name}!'


if __name__ == '__main__':
    # Hämta port från miljövariabel eller använd 5050 som standard
    port = int(os.getenv('PORT', 5050))
    app2.run(host='0.0.0.0', port=port)
