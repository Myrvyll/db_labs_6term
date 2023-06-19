from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# -----------------------------------------------------------------------------------------------
# app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://Ashtar:sunset_clouds@localhost:5433/db_laboratory"
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://Ashtar:sunset_clouds@postgres_db:5432/db_laboratory"
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
