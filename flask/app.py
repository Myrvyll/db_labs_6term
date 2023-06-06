from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
import re

app = Flask(__name__)

# -----------------------------------------------------------------------------------------------
# app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres_db:5432/db_laboratory"
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://Ashtar:sunset_clouds@localhost:5433/db_laboratory"
db = SQLAlchemy(app)

class Exams(db.Model):

    __tablename__ = 'exams'

    outid = db.Column('outid', db.VARCHAR, primary_key=True)
    test = db.Column('test', db.VARCHAR, primary_key=True)
    testyear = db.Column('testyear', db.BigInteger)
    teststatus = db.Column('teststatus', db.VARCHAR)
    ball = db.Column('ball', db.BigInteger)
    ball100 = db.Column('ball100', db.Float)
    ball12 = db.Column('ball12', db.BigInteger)
    ptname = db.Column('ptname', db.VARCHAR)
    ptareaname = db.Column('ptareaname', db.VARCHAR)
    adaptscale = db.Column('adaptscale', db.BigInteger)
    dpalevel = db.Column('dpalevel', db.VARCHAR)
    examlang = db.Column('examlang', db.VARCHAR)
    subtest = db.Column('subtest', db.VARCHAR)
    

    __table_args__ = (
        db.ForeignKeyConstraint(['ptname', 'ptareaname'],
                                ['edfacilities.eoname', 'edfacilities.eoareaname']),
        db.ForeignKeyConstraint(['outid'], 
                                ['students_data.outid'])
    )

    def get_columns(self):
        return [attr for attr in self.__table__.columns]



class StudentsData(db.Model):
    
    __tablename__ = 'students_data'

    outid = db.Column('outid', db.VARCHAR, primary_key=True)
    birth = db.Column('birth', db.BigInteger)
    sextypename = db.Column('sextypename', db.VARCHAR)
    regname = db.Column('regname', db.VARCHAR)
    areaname = db.Column('areaname', db.VARCHAR)
    tername = db.Column('tername', db.VARCHAR)
    regtypename = db.Column('regtypename', db.VARCHAR)
    tertypename = db.Column('tertypename', db.VARCHAR)
    classprofilename = db.Column('classprofilename', db.VARCHAR)
    classlangname = db.Column('classlangname', db.VARCHAR)
    eoname = db.Column('eoname', db.VARCHAR)
    eoareaname = db.Column('eoareaname', db.VARCHAR)

    exams = db.relationship('Exams', backref='StudentsData')

    __table_args__ = (
        db.ForeignKeyConstraint(['eoname', 'eoareaname'],
                                ['edfacilities.eoname', 'edfacilities.eoareaname']),
    )

class Departments(db.Model):

    __tablename__ = 'departments'

    department_name = db.Column('department_name', db.VARCHAR, primary_key=True)

    subordination = db.relationship('Subordination', backref='departments')


class Edfacilities(db.Model):
    __tablename__ = 'edfacilities'

    eoname = db.Column('eoname', db.VARCHAR, primary_key=True)
    eoareaname = db.Column('eoareaname', db.VARCHAR, primary_key=True)
    eoregname = db.Column('eoregname', db.VARCHAR)
    eotername2 = db.Column('eotername2', db.VARCHAR)
    eotername1 = db.Column('eotername1', db.VARCHAR)
    eotypename1 = db.Column('eotypename1', db.VARCHAR)
    eotypename2 = db.Column('eotypename2', db.VARCHAR)

    subordination = db.relationship('Subordination', backref='Edfacilities')
    exams = db.relationship('Exams', backref='Edfacilities')
    students_data = db.relationship('StudentsData', backref='Edfacilities')


class Subordination(db.Model):

    __tablename__ = 'edfacilities_department'

    facility_name = db.Column('facility_name', db.VARCHAR, primary_key = True)
    facility_area = db.Column('facility_area', db.VARCHAR, primary_key = True)
    department_name = db.Column('department_name', db.VARCHAR, primary_key = True)
    

    __table_args__ = (
        db.ForeignKeyConstraint(['department_name'],
                                ['departments.department_name']),
        db.ForeignKeyConstraint(['facility_name', 'facility_area'],
                                ['edfacilities.eoname', 'edfacilities.eoareaname'])
    )

    # def __prep__(self):

     


# -----------------------------------------------------------------------------------------------
@app.route('/')
def index():
    return render_template("index.html")

@app.route('/db_request')
def db_request():
    session = db.session
    years = session.execute(db.select(Exams.testyear).distinct())
    region_names = session.execute(db.select(Edfacilities.eoregname).distinct())
    subject_names = session.execute(db.select(Exams.test).distinct())
    return render_template('request.html', years=years, region_names=region_names, subject_names=subject_names)


row2dict = lambda r: {c.name: str(getattr(r, c.name)) for c in r.__table__.columns}

@app.route('/crud', methods=['GET', 'POST'])
def crud():

    if request.method == 'POST' and request.form.get('create_button') == 'create':
        
        init = {}
        for i in Exams.__table__.columns.keys():
            if request.form.get(i) != '':
                init[i] = request.form.get(i)
            else:
                init[i] = None
        print(init)

        one_exam = Exams(**init)
        if not db.session.execute(db.select(StudentsData).where(StudentsData.outid == init['outid'])):
            student = StudentsData(outid = init['outid'])
            db.session.add(student)
        db.session.add(one_exam)
        db.session.commit()

        print(one_exam.__dict__)

        
    if request.method == 'POST' and (request.form.get('update_button') is not None):
        
        id = request.form.get('update_button')
        id = id.split(', ')
        id[0] = id[0].split("'")[1]
        id[1] = id[1].split("'")[1]
        print(id)

        init = {}
        for i in Exams.__table__.columns.keys():
            if request.form.get(i) != 'None':
                init[i] = request.form.get(i)
            else:
                init[i] = None
        print('-----------------------')
        print('init')
        print(init)
        print('-----------------------')

        one_exam = db.session.execute(db.update(Exams).where(Exams.outid == id[0], Exams.test == id[1])\
                                        .values(**init))

        # for key, item in init.items():
        #     one_exam.key = item



        # for 
        # db.session.add(one_exam)
        db.session.commit()
        # print(request.form.get['update_button'])

        print('im here')
        print(one_exam)  


    if request.method == 'POST' and (request.form.get('delete_button') is not None):
        id = request.form.get('delete_button')
        print(id)
        id = id.split(', ')
        id[0] = id[0].split("'")[1]
        id[1] = id[1].split("'")[1]
        

        db.session.execute(db.delete(Exams).where(Exams.outid == id[0], Exams.test == id[1]))
        db.session.commit()
    
        

    session = db.session
    rows = session.scalars(db.select(Exams).order_by(Exams.outid).limit(10))

    headers = Exams.__table__.columns.keys()

    rows = [(row2dict(i).items(), i.outid, i.test) for i in rows]

    
    return render_template('crud.html', data_table=rows, headers_table=headers)


@app.route('/request_result', methods=['POST'])
def request_result():
    year = request.form.get('year_select')
    region = request.form.get('region_select')
    subject = request.form.get('subject_select')

    session = db.session

    quest_table = db.select(Edfacilities.eoregname, 
                      db.func.max(Exams.ball100).filter(Exams.testyear == 2019).label('max(2019)'), 
                      db.func.max(Exams.ball100).filter(Exams.testyear == year).label('max(2021)'))\
              .join(Edfacilities.exams)\
              .filter(Exams.teststatus=='Зараховано', Exams.test == subject)\
              .group_by(Edfacilities.eoregname)
    data_table = session.execute(quest_table)

    quest_single = db.select(db.func.max(Exams.ball100).filter(Exams.testyear == year).label(f'max({year})'))\
              .join(Edfacilities.exams)\
              .filter(Exams.teststatus=='Зараховано', 
                      Exams.test == subject, 
                      Edfacilities.eoregname == region)
    data_single = session.execute(quest_single).first()
    
    keys = data_table.keys()
    return render_template('request_result.html', year=year, 
                           region=region, subject=subject, max_ball = data_single,
                           data_table=data_table, headers_table=keys)