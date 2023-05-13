-------------
-- Creating table with students data
-------------


CREATE TABLE students_data AS 
  SELECT outid, birth, sextypename, regname, areaname, tername, regtypename, tertypename, classprofilename, classlangname, eoname, eoareaname FROM ZNOData;
ALTER TABLE students_data ADD PRIMARY KEY (outid);


------------
-- Create tables, which are connected to educational facilities. There are 2 tables for separate entities: school and educational department.
-- Table edfacilities_department describes many-to-many connection between them.
------------


-- create table with all possible educational facilities
CREATE TEMP TABLE eds AS
  SELECT DISTINCT eoname, eotypename, eoregname, eoareaname, eotername, eoparent FROM znodata;

INSERT INTO eds (eoname, eoregname, eoareaname, eotername) 
  SELECT ukrptname, ukrptregname, ukrptareaname, ukrpttername FROM znodata where ukrptname is not null
  UNION ALL
  SELECT histptname, histptregname, histptareaname, histpttername FROM znodata where histptname is not null
  UNION ALL
  SELECT mathptname, mathptregname, mathptareaname, mathpttername FROM znodata where mathptname is not null
  UNION ALL
  SELECT physptname, physptregname, physptareaname, physpttername FROM znodata where physptname is not null
  UNION ALL
  SELECT chemptname, chemptregname, chemptareaname, chempttername FROM znodata where chemptname is not null
  UNION ALL
  SELECT bioptname, bioptregname, bioptareaname, biopttername FROM znodata where bioptname is not null
  UNION ALL
  SELECT geoptname, geoptregname, geoptareaname, geopttername FROM znodata where geoptname is not null
  UNION ALL
  SELECT engptname, engptregname, engptareaname, engpttername FROM znodata where engptname is not null
  UNION ALL
  SELECT fraptname, fraptregname, fraptareaname, frapttername FROM znodata where fraptname is not null
  UNION ALL
  SELECT deuptname, deuptregname, deuptareaname, deupttername FROM znodata where deuptname is not null
  UNION ALL
  SELECT spaptname, spaptregname, spaptareaname, spapttername FROM znodata where spaptname is not null
  UNION ALL
  SELECT mathstptname, mathstptregname, mathstptareaname, mathstpttername FROM znodata where mathstptname is not null
  UNION ALL
  SELECT umlptname, umlptregname, umlptareaname, umlpttername FROM znodata where umlptname is not null;

-- remove double records
create temp table eds2 as
select distinct * from eds;

-- drop all doubles with unfull information
delete from eds2
where eotypename is null and
    eoname || eoareaname in (select eoname || eoareaname from eds2 where eotypename is not null);


-- get all departments into separate table
create table departments as
  select distinct eoparent as department_name 
  from eds2 
  where eoparent is not null;
  
-- get all unique pairs department-school into separate table
create table edfacilities_department AS 
  select distinct eoname as facility_name, eoareaname as facility_area, eoparent as department_name 
  from eds2
  where eoparent is not null;

-- drop departments from table with schools
alter table eds2 drop eoparent;

-- select unique school-type-placement pairs
create temp table edfacilities2 as
  select DISTINCT * FROM eds2;


-- drop empty row
delete from edfacilities2 where eoname is null;


-- get all doubles and counts into separate table
create temp table facility_counter as
select eoname, eoareaname, count(*) as name_count from edfacilities2 group by eoname, eoareaname having count (*) > 1;


-- get all records which are doubled
create temp table doubles as
select * from edfacilities2 where eoname in (select eoname from facility_counter) AND eoareaname in (select eoareaname from facility_counter);


create table edfacilities as
select edfacilities2.eoname, 
	     edfacilities2.eoareaname, 
       edfacilities2.eoregname,
       edfacilities2.eotername AS eotername1, 
	     CASE WHEN edfacilities2.eotername = doubles.eotername THEN null
            ELSE doubles.eotername
	     END as eotername2,
	     edfacilities2.eotypename as eotypename1, 
	     CASE WHEN edfacilities2.eotypename = doubles.eotypename THEN null
            ELSE doubles.eotypename
	     END as eotypename2
from edfacilities2 left join doubles on edfacilities2.eoname = doubles.eoname and edfacilities2.eoareaname = doubles.eoareaname
where (doubles.eoname is null or
       edfacilities2.eotypename is null or
       edfacilities2.eotypename > doubles.eotypename or 
	   edfacilities2.eotername > doubles.eotername);


alter table departments add PRIMARY key (department_name);
alter table edfacilities add PRIMARY key (eoname, eoareaname);
alter table edfacilities_department add PRIMARY key (facility_name, facility_area, department_name);

ALTER table edfacilities_department add constraint fk_edfacilities_department_departments foreign key (department_name) references departments (department_name);
ALTER table edfacilities_department add constraint fk_edfacilities_department_edfacilities foreign key  (facility_name, facility_area) references edfacilities (eoname, eoareaname);
alter table students_data add constraint fk_students_data_edfacility foreign key  (eoname, eoareaname) references edfacilities (eoname, eoareaname);



---------------
-- Creates table for exams, 1 for all subjects.
---------------



-- drop table if exists exams;
create table exams AS
    select outid,
        testyear, 
        umltest as test,
        umlteststatus as teststatus,
		    umlball as ball,
        umlball100 as ball100,
	      umlball12 as ball12,
        ukrptname as ptname,
        ukrptareaname as ptareaname,
		    umladaptscale as adaptscale,
	   	  engdpalevel as dpalevel,
        physlang as examlang,
		    ukrsubtest as subtest from znodata
with no data;


insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, adaptscale, subtest, ptname, ptareaname) 
    select outid, testyear, ukrtest, ukrteststatus, ukrball, ukrball100, ukrball12, ukradaptscale, ukrsubtest, ukrptname, ukrptareaname from znodata 
	where ukrtest is not null;
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, ptname, examlang, ptareaname) 
    select outid, testyear, histtest, histteststatus, histball, histball100, histball12, histptname, histlang, histptareaname from znodata 
	where histtest is not null;
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, dpalevel, ptname, examlang, ptareaname) 
    select outid, testyear, mathtest, mathteststatus, mathball, mathball100, mathball12, mathdpalevel, mathptname, mathlang, mathptareaname from znodata 
	where mathtest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, ptname, examlang, ptareaname) 
    select outid, testyear, phystest, physteststatus, physball, physball100, physball12, physptname, physlang, physptareaname from znodata 
	where phystest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, ptname, examlang, ptareaname) 
    select outid, testyear, chemtest, chemteststatus, chemball, chemball100, chemball12, chemptname, chemlang, chemptareaname from znodata 
	where chemtest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, ptname, examlang, ptareaname) 
    select outid, testyear, biotest, bioteststatus, bioball, bioball100, bioball12, bioptname, biolang, bioptareaname from znodata 
	where biotest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, ptname, examlang, ptareaname) 
    select outid, testyear, geotest, geoteststatus, geoball, geoball100, geoball12, geoptname, geolang, geoptareaname from znodata 
	where geotest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, dpalevel, ptname, ptareaname) 
    select outid, testyear, engtest, engteststatus, engball, engball100, engball12, engdpalevel, engptname, engptareaname from znodata 
	where engtest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, dpalevel, ptname, ptareaname) 
     select outid, testyear, fratest, frateststatus, fraball, fraball100, fraball12, fradpalevel, fraptname, fraptareaname from znodata 
	where fratest is not null;

	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, dpalevel, ptname, ptareaname) 
     select outid, testyear, deutest, deuteststatus, deuball, deuball100, deuball12, deudpalevel, deuptname, deuptareaname from znodata 
	where deutest is not null;

	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, dpalevel, ptname, ptareaname) 
     select outid, testyear, spatest, spateststatus, spaball, spaball100, spaball12, spadpalevel, spaptname, spaptareaname from znodata 
	where spatest is not null;

	
insert into exams (outid, testyear, test, teststatus, ball, ball12, examlang, ptname, ptareaname) 
    select outid, testyear, mathsttest, mathstteststatus, mathstball, mathstball12, mathstlang, mathstptname, mathstptareaname from znodata  
	where mathsttest is not null;
	
	
insert into exams (outid, testyear, test, teststatus, ball, ball100, ball12, adaptscale, ptname, ptareaname) 
     select outid, testyear, umltest, umlteststatus, umlball, umlball100, umlball12, umladaptscale, umlptname, umlptareaname from znodata 
	where umltest is not null;


alter table exams add PRIMARY key (outid, test);
ALTER table exams add constraint fk_exams_students_data foreign key (outid) references students_data (outid);
ALTER table exams add constraint fk_exams_edfacilities foreign key (ptname, ptareaname) references edfacilities (eoname, eoareaname);



