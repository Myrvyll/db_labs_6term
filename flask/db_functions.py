from model import *
import redis

redis_client = redis.Redis(host='redis_lab')


def get_distinct_column_values(column_name):

    result = db.session.execute(db.select(column_name).distinct())
    list_of_values = [str(i[0]) for i in result]

    return list_of_values


def get_request_result(year, region, subject, func):

    function = None
    data_single = None
    
    if redis_client.get(f'{year}:{region}:{subject}:{func}/aggregated'):
        print('boop-beep from redis cache')
        data_single = redis_client.get(f'{year}:{region}:{subject}:{func}/aggregated').decode()
        if data_single == 'None':
            return None
        else:
            data_single = float(data_single)
    else:
        session = db.session
    
        if func == 'Max':
            function = db.func.max
        elif func == 'Min':
            function = db.func.min
        elif func == 'Avg':
            function = db.func.avg
        else:
            raise TypeError('This function work only with Max, Min, Avg functions')
    
        quest_single = db.select(function(Exams.ball100).filter(Exams.testyear == year).label(f'{func}({year})'))\
                  .join(Edfacilities.exams)\
                  .filter(Exams.teststatus=='Зараховано', 
                          Exams.test == subject, 
                          Edfacilities.eoregname == region)
        data_single = session.execute(quest_single).first()._asdict()[f'{func}({year})']
        if data_single is None:
            redis_client.set(f'{year}:{region}:{subject}:{func}/aggregated', 'None', ex=60)
        else:
            redis_client.set(f'{year}:{region}:{subject}:{func}/aggregated', data_single, ex=60)

    return data_single


# -------------------------------------------------------------------------------------------------------------------------
def create_departments(init):
    department_row = Departments(**init)
    db.session.add(department_row)
    db.session.commit()


def update_departments(id, init):
    db.session.execute(db.update(Departments).where(Departments.department_name == id)\
                         .values(**init))
    db.session.commit()

def delete_departments(id):
    db.session.execute(db.delete(Departments).where(Departments.department_name == id))
    db.session.commit()


# -------------------------------------------------------------------------------------------------------------------------
def create_edfacilities(init):
    edfacility_row = Edfacilities(**init)
    db.session.add(edfacility_row)
    db.session.commit()


def update_edfacilities(id, init):
    id = id.split(';')
    db.session.execute(db.update(Edfacilities).where(Edfacilities.eoname == id[0], Edfacilities.eoareaname == id[1])\
                         .values(**init))
    db.session.commit()


def delete_edfacilities(id):
    id = id.split(';')
    db.session.execute(db.delete(Edfacilities).where(Edfacilities.eoname == id[0], Edfacilities.eoareaname == id[1]))
    db.session.commit()


# -------------------------------------------------------------------------------------------------------------------------
def create_exams(init):
    exams_row = Exams(**init)

    if init['outid'] is not None:
        if not db.session.execute(db.select(StudentsData).where(StudentsData.outid == init['outid'])).first():
            student = StudentsData(outid = init['outid'])
            db.session.add(student)
    if (init['ptname'] is not None) or (init['ptareaname'] is not None):
        if not db.session.execute(db.select(Edfacilities).where(Edfacilities.eoname == init['ptname'], Edfacilities.eoareaname == init['ptareaname'])).first():
            institution = Edfacilities(eoname = init['ptname'], eoareaname=init['ptareaname'])
            db.session.add(institution)
    
    db.session.add(exams_row)
    db.session.commit()


def update_exams(id, init):
    id = id.split(';')

    if init['outid'] is not None:
        if not db.session.execute(db.select(StudentsData).where(StudentsData.outid == init['outid'])).first():
            student = StudentsData(outid = init['outid'])
            db.session.add(student)
    if (init['ptname'] is not None) or (init['ptareaname'] is not None):
        if not db.session.execute(db.select(Edfacilities).where(Edfacilities.eoname == init['ptname'], Edfacilities.eoareaname == init['ptareaname'])).first():
            institution = Edfacilities(eoname = init['ptname'], eoareaname=init['ptareaname'])
            db.session.add(institution)
    
    db.session.execute(db.update(Exams).where(Exams.outid == id[0], Exams.test == id[1])\
                         .values(**init))
    db.session.commit()


def delete_exams(id):
    id = id.split(';')
    db.session.execute(db.delete(Exams).where(Exams.outid == id[0], Exams.test == id[1]))
    db.session.commit()


# -------------------------------------------------------------------------------------------------------------------------
def create_students(init):

    one_student = StudentsData(**init)
    if (init['eoname'] is not None) or (init['eoareaname'] is not None):
        if not db.session.execute(db.select(Edfacilities).where(Edfacilities.eoname == init['eoname'], Edfacilities.eoareaname == init['eoareaname'])).first():
            institution = Edfacilities(eoname = init['eoname'], eoareaname=init['eoareaname'])
            db.session.add(institution)
    db.session.add(one_student)
    db.session.commit()


def update_students(id, init):
    if (init['eoname'] is not None) or (init['eoareaname'] is not None):      
        if not db.session.execute(db.select(Edfacilities).where(Edfacilities.eoname == init['eoname'], Edfacilities.eoareaname == init['eoareaname'])).first():
            institution = Edfacilities(eoname = init['eoname'], eoareaname=init['eoareaname'])
            db.session.add(institution)
    
    db.session.execute(db.update(StudentsData).where(StudentsData.outid == id)\
                         .values(**init))
    db.session.commit()


def delete_students(id):
    db.session.execute(db.delete(StudentsData).where(StudentsData.outid == id))
    db.session.commit()


# -------------------------------------------------------------------------------------------------------------------------
def create_subordination(init):
    connection = Subordination(**init)

    if (init['facility_name'] is not None) or (init['facility_area'] is not None):
        if not db.session.execute(db.select(Edfacilities).where(Edfacilities.eoname == init['facility_name'], Edfacilities.eoareaname == init['facility_area'])).first():
            institution = Edfacilities(eoname = init['facility_name'], eoareaname=init['facility_area'])
            db.session.add(institution)
    if (init['department_name'] is not None):
        if not db.session.execute(db.select(Departments).where(Departments.department_name == init['department_name'])).first():
            department = Departments(department_name = init['department_name'])
            db.session.add(department)

    db.session.add(connection)
    db.session.commit()


def update_subordination(id, init):
    id = id.split(';')

    if (init['facility_name'] is not None) or (init['facility_area'] is not None):
        if not db.session.execute(db.select(Edfacilities).where(Edfacilities.eoname == init['facility_name'], Edfacilities.eoareaname == init['facility_area'])).first():
            institution = Edfacilities(eoname = init['facility_name'], eoareaname=init['facility_area'])
            db.session.add(institution)
    if (init['department_name'] is not None):  
        if not db.session.execute(db.select(Departments).where(Departments.department_name == init['department_name'])).first():
            department = Departments(outid = init['department_name'])
            db.session.add(department)

    db.session.execute(db.update(Subordination).where(Subordination.facility_name == id[0],\
                                                      Subordination.facility_area == id[1],\
                                                      Subordination.department_name == id[2])\
                         .values(**init))
    db.session.commit()


def delete_subordination(id):
    id = id.split(';')
    print(id)
    db.session.execute(db.delete(Subordination).where(Subordination.facility_name == id[0],\
                                                      Subordination.facility_area == id[1],\
                                                      Subordination.department_name == id[2]))
    db.session.commit()





