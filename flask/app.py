from flask import render_template, request
from model import *
from db_functions import *

# -------------------------------------------------------------------------------------------------------------------------
row2dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}


def is_not_empty(string):
    if (string is not None) and \
       (string != 'None') and \
       (string != ' ') and \
       (string != ''):
        return True
    else:
        return False


def get_attr_dictionary(form, class_keys):
    attr_dictionary = {}
    for i in class_keys:
        if is_not_empty(form.get(i)):
            attr_dictionary[i] = form.get(i)
        else:
            attr_dictionary[i] = None
    return attr_dictionary


# -------------------------------------------------------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template("index.html")


# -------------------------------------------------------------------------------------------------------------------------
@app.route('/db_request', methods=['POST', 'GET'])
def db_request():

    years = get_distinct_column_values(Exams.testyear)
    region_names = get_distinct_column_values(Edfacilities.eoregname)
    subject_names = get_distinct_column_values(Exams.test)
    functions = ['Min', 'Max', 'Avg']

    if request.method == 'POST':
        year = request.form.get('year_select')
        region = request.form.get('region_select')
        subject = request.form.get('subject_select')
        function = request.form.get('func_select')
    
        data_single = get_request_result(year, region, subject, function)
        
        return render_template('request.html', years=years, region_names=region_names, subject_names=subject_names, function_names=functions,
                                year=year, region=region, subject=subject, aggreg_ball=data_single,
                                function=function)
 
    return render_template('request.html', years=years, region_names=region_names, subject_names=subject_names, function_names=functions)



# -------------------------------------------------------------------------------------------------------------------------
@app.route('/departments', methods=['GET', 'POST'])
def departments():
    headers = Departments.__table__.columns.keys()

    if request.method == 'POST' and request.form.get('create_button') == 'create':
        init = get_attr_dictionary(request.form, headers)
        create_departments(init)

    if request.method == 'POST' and (request.form.get('update_button') is not None):
        id = request.form.get('update_button')
        init = get_attr_dictionary(request.form, headers)
        update_departments(id, init)

    if request.method == 'POST' and (request.form.get('delete_button') is not None):
        id = request.form.get('delete_button')
        delete_departments(id)

    session = db.session
    rows = session.scalars(db.select(Departments).order_by(Departments.department_name).limit(10))
    rows = [(row2dict(i).items(), i.department_name) for i in rows]

    return render_template('departments.html', data_table=rows, headers_table=headers)


# -------------------------------------------------------------------------------------------------------------------------
@app.route('/edfacilities', methods=['GET', 'POST'])
def edfacilities():
    headers = Edfacilities.__table__.columns.keys()

    if request.method == 'POST' and request.form.get('create_button') == 'create':
        init = get_attr_dictionary(request.form, headers)
        create_edfacilities(init)

    if request.method == 'POST' and (request.form.get('update_button') is not None):
        id = request.form.get('update_button')
        init = get_attr_dictionary(request.form, headers)
        update_edfacilities(id, init)

    if request.method == 'POST' and (request.form.get('delete_button') is not None):
        id = request.form.get('delete_button')
        delete_edfacilities(id)

    rows = db.session.scalars(db.select(Edfacilities).order_by(Edfacilities.eoname).limit(10))
    rows = [(row2dict(i).items(), f'{i.eoname};{i.eoareaname}') for i in rows]

    return render_template('edfacilities.html', data_table=rows, headers_table=headers)


# -------------------------------------------------------------------------------------------------------------------------
@app.route('/exams', methods=['GET', 'POST'])
def exams():
    headers = Exams.__table__.columns.keys()

    if request.method == 'POST' and request.form.get('create_button') == 'create':
        init = get_attr_dictionary(request.form, headers)
        create_exams(init)

    if request.method == 'POST' and (request.form.get('update_button') is not None):
        id = request.form.get('update_button')
        init = get_attr_dictionary(request.form, headers)
        update_exams(id, init)
    
    if request.method == 'POST' and (request.form.get('delete_button') is not None):
        id = request.form.get('delete_button')
        delete_exams(id)
    
    session = db.session
    rows = session.scalars(db.select(Exams).order_by(Exams.outid).limit(10))
    rows = [(row2dict(i).items(), f'{i.outid};{i.test}') for i in rows]

    return render_template('exams.html', data_table=rows, headers_table=headers)


# -------------------------------------------------------------------------------------------------------------------------
@app.route('/students', methods=['GET', 'POST'])
def students():
    headers = StudentsData.__table__.columns.keys()

    if request.method == 'POST' and request.form.get('create_button') == 'create':
        init = get_attr_dictionary(request.form, headers)
        create_students(init)

    if request.method == 'POST' and (request.form.get('update_button') is not None):
        id = request.form.get('update_button')
        init = get_attr_dictionary(request.form, headers) 
        update_students(id, init)

    if request.method == 'POST' and (request.form.get('delete_button') is not None):
        id = request.form.get('delete_button')
        delete_students(id)


    rows = db.session.scalars(db.select(StudentsData).order_by(StudentsData.outid).limit(10))
    rows = [(row2dict(i).items(), i.outid) for i in rows]

    return render_template('students.html', data_table=rows, headers_table=headers)


# -------------------------------------------------------------------------------------------------------------------------
@app.route('/subordination', methods=['GET', 'POST'])
def subordination():
    headers = Subordination.__table__.columns.keys()

    if request.method == 'POST' and request.form.get('create_button') == 'create':
        init = get_attr_dictionary(request.form, headers)
        create_subordination(init)

    if request.method == 'POST' and (request.form.get('update_button') is not None):
        id = request.form.get('update_button')
        init = get_attr_dictionary(request.form, headers)
        update_subordination(id, init)

    if request.method == 'POST' and (request.form.get('delete_button') is not None):
        id = request.form.get('delete_button')
        delete_subordination(id)

    rows = db.session.scalars(db.select(Subordination).order_by(Subordination.facility_name).limit(10))
    rows = [(row2dict(i).items(), f'{i.facility_name};{i.facility_area};{i.department_name}') for i in rows]

    return render_template('subordination.html', data_table=rows, headers_table=headers)

