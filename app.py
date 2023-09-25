from flask import Flask, jsonify, request, redirect
from bson.json_util import dumps
from bson import json_util, objectid
import json
import random
from services import MongoDatabase

app = Flask(__name__)
app.config.debug = True

DB = MongoDatabase()

def clean(data):
    return json.loads(json.dumps(data, sort_keys=True, indent=4, default=json_util.default))

def ID(num):
    return objectid.ObjectId(num)

def get_quiz_with_attr(field, value):
    return DB.find_quiz(field, value)

def unpack_record(record, sublist, fn):
    new_record = record
    new_record[sublist] = list(map(fn, record[sublist]))
    return new_record

def unpack_quiz(record):
    return unpack_record(record, "questions", unpack_question)

def unpack_question(record):
    question = DB.find_question("_id", ID(record["$oid"]))
    return unpack_record(question, "answers", unpack_answer)

def unpack_answer(oid):
    return DB.find_answer("_id", ID(oid))

@app.route('/')
@app.route('/index')
def index():
    return "Welcome to the Quibble API!"

@app.route('/quizzes',  methods=['GET'])
@app.route('/quizzes/',  methods=['GET'])
@app.route('/quizzes/all',  methods=['GET'])
def get_all_quizzes():
    data = clean(DB.get_all(DB.quizzes()))
    #print(data)
    return {"result":data}

@app.route('/quizzes/id/<string:oid>', methods=['GET'])
@app.route('/quizzes/id/<string:oid>/', methods=['GET'])
def get_quiz_by_id(oid):
    data = clean(DB.find_quiz("_id", ID(oid)))
    quiz = clean(unpack_quiz(data))

    if quiz:
        return quiz
    else:
        return "Quiz not found"

@app.route('/quizzes/name/<string:name>', methods=['GET'])
@app.route('/quizzes/name/<string:name>/', methods=['GET'])
def get_quiz_by_name(name):
    address = "https://www.wizardingworld.com/quiz/" + name
    data = clean(DB.find_quiz("address", address))
    quiz = clean(unpack_quiz(data))

    if quiz:
        return quiz
    else:
        return "Quiz not found"

def get_quiz_question(quiz_data, num):
    question_data = quiz_data.get("questions")
    if num:
        if num < len(question_data) and 0 < num:
            try:
                return clean(question_data[num-1])
                #id = str(quiz_data[num-1]['$oid'])
                #question = DB.find_question("_id",ID(id))
                #return clean(question)
            except:
                return "Question not found"
        else:
            return "Question out of bounds"
    else:
        return jsonify(quiz_data)

@app.route('/quizzes/id/<string:oid>/questions', methods=['GET'])
@app.route('/quizzes/id/<string:oid>/questions/<int:question_num>', methods=['GET'])
def get_quiz_question_by_id(oid, question_num = None):
    quiz_data = get_quiz_by_id(ID(oid))
    return get_quiz_question(quiz_data, question_num)

@app.route('/quizzes/name/<string:name>/questions', methods=['GET'])
@app.route('/quizzes/name/<string:name>/questions/<int:question_num>', methods=['GET'])
def get_quiz_question_by_name(name, question_num = None):
    address = "https://www.wizardingworld.com/quiz/" + name
    quiz_data = get_quiz_by_name(name)

    return get_quiz_question(quiz_data, question_num)

def generate_rand_quiz_id():
    quizzes = get_all_quizzes()["result"]
    index = random.randint(0, len(quizzes)-1)
    quiz = quizzes[index]
    return clean(quiz)["_id"]["$oid"]

@app.route('/random', methods=['GET'])
@app.route('/random/quiz', methods=['GET'])
@app.route('/quizzes/random', methods=['GET'])
def get_random_quiz():
    oid = generate_rand_quiz_id()
    quiz = get_quiz_by_id(oid)
    if not quiz.get("complete"):
        get_random_quiz()
    else:
        return redirect(f'/quizzes/id/{oid}', code=302)

@app.route('/random/question', methods=['GET'])
def get_random_question():
    oid = generate_rand_quiz_id()
    quiz = get_quiz_by_id(oid)
    index = random.randint(0, len(quiz.get("questions"))-1)
    return redirect(f'/quizzes/id/{oid}/questions/{index}', code=302)

@app.route('/addresses/all',  methods=['GET'])
@app.route('/addresses',  methods=['GET'])
@app.route('/addresses/',  methods=['GET'])
def get_addresses():
    addresses = []
    quizzes = DB.get_all(DB.quizzes())
    for quiz in quizzes:
        addresses.append(quiz["address"])
    return jsonify(addresses)

@app.route('/addresses/parsed',  methods=['GET'])
def get_parsed():
    return jsonify(clean(DB.get_record_list(DB.quizzes(), "complete", True, "address")))

if __name__ == '__main__':
    app.debug = True
    app.run()
