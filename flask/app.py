from flask import Flask, render_template, request
from flask_sqlalchemy import SQLAlchemy
import sqlalchemy
from sqlalchemy.ext.automap import automap_base

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

@app.route('/crud')
def crud():
    session = db.session
    rows = tuple(session.execute(db.select(Exams).limit(1)))

    headers = Exams.__table__.columns.keys()
    print(rows)
    

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
    
    # descr = data.column_descriptions
    keys = data_table.keys()
    return render_template('request_result.html', year=year, 
                           region=region, subject=subject, max_ball = data_single,
                           data_table=data_table, headers_table=keys)