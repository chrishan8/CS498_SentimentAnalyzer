import cs410proj as sentiment
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    if not request.json or not 'text' in request.json:
        abort(400)
    rating = sentiment.get_rating(request.json['text'])
    return jsonify({'sentiment': rating}), 201

if __name__ == '__main__':
    app.run(debug=True)
