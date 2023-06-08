# Импортируем необходимые библиотеки и модули
import csv
import os
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy

# Создаем экземпляр приложения Flask
app = Flask(__name__)

# Определяем конфигурацию базы данных
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Создаем экземпляр объекта SQLAlchemy
db = SQLAlchemy(app)


# Создаем модель для хранения информации о файлах
class FileModel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(100), nullable=False)
    columns = db.Column(db.String(500), nullable=False)

    def __repr__(self):
        return f"File(filename='{self.filename}', columns='{self.columns}')"


# Создаем таблицы в базе данных
db.create_all()


# Определяем функцию для загрузки файла
def save_file(file):
    filename = file.filename
    file.save(os.path.join(app.root_path, 'uploads', filename))
    return filename


# Определяем функцию для получения списка файлов с информацией о колонках
def get_file_list():
    file_list = []
    for file in FileModel.query.all():
        file_list.append({'id': file.id, 'filename': file.filename, 'columns': file.columns})
    return file_list


# Определяем функцию для получения данных из конкретного файла
def get_data(file_id, filters=None, sort_by=None):
    file = FileModel.query.filter_by(id=file_id).first()
    if not file:
        return {'message': 'File not found'}, 404
    filename = file.filename
    columns = file.columns.split(',')

    with open(os.path.join(app.root_path, 'uploads', filename), 'r') as f:
        reader = csv.DictReader(f)
        if filters:
            for f in filters:
                reader = filter(lambda row: row[f['column']] == f['value'], reader)
        if sort_by:
            reader = sorted(reader, key=lambda row: row[sort_by['column']], reverse=sort_by['reverse'])
        data = []
        for row in reader:
            data.append(dict((k, row[k]) for k in columns))
    return data


# Определяем маршруты REST API
# Загрузка файла
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return {'message': 'No file specified'}, 400

    file = request.files['file']
    if file.filename == '':
        return {'message': 'Empty filename'}, 400

    filename = save_file(file)
    # Определяем колонки файла
    with open(os.path.join(app.root_path, 'uploads', filename), 'r') as f:
        reader = csv.reader(f)
        columns = ','.join(next(reader))
    # Сохраняем информацию о файле и его колонках в базу данных
    file_model = FileModel(filename=filename, columns=columns)
    db.session.add(file_model)
    db.session.commit()
    return {'message': 'File uploaded successfully'}, 201


# Получение списка файлов
@app.route('/files', methods=['GET'])
def get_files():
    file_list = get_file_list()
    return jsonify(file_list), 200


# Получение данных из файла
@app.route('/data/<int:file_id>', methods=['GET'])
def get_file_data(file_id):
    filters = None
    if 'filters' in request.args:
        filters = []
        for f in request.args.getlist('filters'):
            column, value = f.split(':')
            filters.append({'column': column, 'value': value})
    sort_by = None
    if 'sort_by' in request.args:
        column, reverse = request.args['sort_by'].split(':')
        sort_by = {'column': column, 'reverse': True if reverse == 'desc' else False}
    data = get_data(file_id, filters, sort_by)
    return jsonify(data), 200



# Запуск приложения
if __name__ == '__main__':
    app.run(debug=True)
