import analyzer as sentiment
from flask import Flask, jsonify, request

app = Flask(__name__)

@app.route('/api/analyze', methods=['POST'])
def analyze():
    if not request.json or not 'text' in request.json:
        abort(400)
    wordBag = request.json['text'].split(" ")
    rating = sentiment.get_rating(wordBag)
    return jsonify({'sentiment': rating}), 201

if __name__ == '__main__':
    app.run(debug=True)
