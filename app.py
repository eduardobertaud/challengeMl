from flask import Flask
from flask import abort
import spark_app as s

app = Flask(__name__)


@app.route('/users/<id>')
def get_user(id):
    if id.isdigit():
        resp = s.getUser(id)
        if resp:
            return resp
    else:
        abort(400, "Error")


if __name__ == '__main__':
    app.run(debug=True)
