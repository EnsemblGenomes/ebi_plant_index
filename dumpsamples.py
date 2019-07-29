#!/nfs/production/panda/ensemblgenomes/development/mrossello/python_installation/bin/python

import sqlite3
from sqlite3 import Error
import logging
import json
import os
import re
import time
import sys

class DumpSamples:

    def __init__(self,db_file,jsonf,jsongp,att_list = [],brapi_eq = []):
        self.__open(db_file,jsonf,jsongp)
        self.writing = False #to know if to add comma when writing studies to json
        self.start = time.time()
        self.study_count = 1
        if len(att_list) != len(brapi_eq):
            self.brapi_eq = [] # if no brapi list supplied then use att_list for bfield
        else:
            self.brapi_eq = brapi_eq 
        self.att_list = att_list
        self.brapi_eq = brapi_eq
        self.__setUp()

    def __open(self,db_file,jsonf,jsongp): 
        try:
            self.conn = sqlite3.connect(db_file)
        except Error as e:
            logging.error('problem connecting to db {}. {}'.format(db_file,e))
            raise SystemExit
        self.f = open(jsonf,"w+")
        self.f.write('[')
        self.gpf = open(jsongp,"w+")
        self.gpf.write('[')

    def __setUp(self): #add column number check for tables here
        logging.info('entering set up: 1.) remove unnecessary attributes 2.) create study_group table 3.) create extra_study_group table 4.) create GP table and GP_MULT table')
        set_up_curs = self.conn.cursor()
        command = 'delete from attributes where field in (?,?,?,?)'
        terms = ('ena-last-update','ena-base-count','ena-spot-count','ena-first-public')
        set_up_curs.execute(command,terms)
        self.conn.commit()

        logging.info('2.) creating study_group table')
        set_up_curs.execute('DROP TABLE IF EXISTS study_group')
        #create temporary study_group without column DATA_COUNT. doing in 2 steps helped with the logic and also I was unable to add the column directly (instead of parse into new table). 
        set_up_curs.execute('CREATE TABLE TEMP_STUDY_GROUP(STUDY TEXT PRIMARY KEY, AVG_FIELD REAL, SAMPLE_CNT INTEGER, PROJECT TEXT)')
        set_up_curs.execute('INSERT INTO TEMP_STUDY_GROUP (study, avg_field, sample_cnt,project) select study.study as erp, avg(fc.field_c) as avc, count(fc.id) as cnt, study.project as prj from  (  select sample.study as study, sample.id as id, count(field) as field_c from sample join attributes using(id) where sample.study is not null  group by sample.study, sample.id  ) fc, study where (fc.study = study.study) group by fc.study UNION select study.study as erp, avg(fc.field_c) as avc, count(fc.id) as cnt, study.project as prj from  (  select sample.study as study, sample.id as id, count(field) as field_c from   sample join attributes using(id) where sample.study is not null  group by sample.study, sample.id  ) fc, study where (fc.study = study.project) and (study.study != study.project) group by fc.study order by avc desc')
        set_up_curs.execute('CREATE TABLE STUDY_GROUP(STUDY TEXT PRIMARY KEY, AVG_FIELD REAL, SAMPLE_CNT INTEGER, PROJECT TEXT, DATA_COUNT INTEGER)')
        set_up_curs.execute('INSERT INTO STUDY_GROUP (study, avg_field, sample_cnt,project,data_count) select temp_study_group.study, temp_study_group.avg_field, temp_study_group.sample_cnt, temp_study_group.project, count(*) from temp_study_group join sample on (temp_study_group.study = sample.study) join data on (sample.id = data.id) group by temp_study_group.study union select temp_study_group.study, temp_study_group.avg_field, temp_study_group.sample_cnt, temp_study_group.project, count(*) from temp_study_group join sample on (temp_study_group.project = sample.study) join data on (sample.id = data.id) group by temp_study_group.study order by temp_study_group.avg_field desc')
        set_up_curs.execute('DROP TABLE TEMP_STUDY_GROUP')
        self.conn.commit()

        logging.info('3.) creating extra_study_group table')
        set_up_curs.execute('DROP TABLE IF EXISTS extra_study_group')
        set_up_curs.execute('CREATE TABLE EXTRA_STUDY_GROUP(PROJECT TEXT PRIMARY KEY, DATA_COUNT INTEGER, STUDY TEXT, SAMPLE_COUNT)')
        set_up_curs.execute('INSERT INTO EXTRA_STUDY_GROUP (project, data_count, study, sample_count) select study.project,count(data.id),study.study, count(distinct sample.id)from sample join data using (id), study where sample.study is null and (data.study = study.project) group by data.study UNION select study.project,count(data.id),study.study, count(distinct sample.id) from sample join data using (id), study where sample.study is null and (data.study = study.study) and (study.study != study.project) group by data.study' )
        self.conn.commit()

        logging.info('4.) creating GP table and GP_MULT table')
        set_up_curs.execute('DROP TABLE IF EXISTS GP')
        set_up_curs.execute('DROP TABLE IF EXISTS GP_MULT')
        table_gp = 'CREATE TABLE GP (GP_ID TEXT NOT NULL PRIMARY KEY, GERMPLASMNAME TEXT, GENUS TEXT, SPECIES TEXT)'
        table_gp_mult = 'CREATE TABLE GP_MULT(GERMPLASMNAME TEXT, BFIELD TEXT, VALUE TEXT, TYPE TEXT,PRIMARY KEY(GERMPLASMNAME,BFIELD,VALUE))'
        set_up_curs.execute(table_gp)
        set_up_curs.execute(table_gp_mult)
        self.conn.commit()

        set_up_curs.close()
 
    def studyList(self, slist = [], t2list = []): #alternative to perStudy(), provide a list of studies
        full_list = slist + t2list
        enterT2 = len(slist)
        comma_pending = False
        if len(full_list) < 1:
            logging.info('studyList(slist,t2list) requires a list of studies (not empty list)')
            return
        curs = self.conn.cursor()
        last_element = full_list[-1:]
        counter = 0
        for study in full_list:
            curs.execute('select project,study from study where project = ? or study = ?',(study,study))
            row = curs.fetchone()
            if row is None:
                logging.info('study %s not found in db' % study)
                counter += 1
                continue
            if comma_pending:
                self.f.write(',')
            erp = row[1]
            prj = row[0]
            s = Study(erp,prj)
            if counter < enterT2:
                self.__fillStudy(s,1) 
            else:
                self.__fillStudy(s,2)
            counter += 1
            self.f.write(s.printJ())
            if last_element != erp or last_element != prj:
                comma_pending = True
#            else:
#                comma_pending = False
            logging.info('at {} seconds, processed project {}/{}'.format(time.time() - self.start,prj,erp))
        curs.close()
        self.conn.commit() #insert statements made on db during above main loop


    def perStudy(self,s_type = 1): #hardcoded use of study_group and extra_study_group tables
        pmt = ()
        if s_type == 1:
            query = 'select project,study from study_group where avg_field > ?'
            pmt = (4,) 
        else:
            query = 'select project,study from extra_study_group' 
        studies = self.__iterator(query,pmt)
        if self.writing:
            self.f.write(',')
        else:
            self.writing = True
        for row,next in studies:
            erp = row[1]
            prj = row[0]
            now = time.time() - self.start
            if (not self.study_count % 200):
                logging.info('at {} seconds, processing project number {}({}/{})'.format(now,self.study_count,prj,erp))
            s = Study(erp,prj)
            self.__fillStudy(s,s_type)
            self.f.write(s.printJ())
            self.study_count += 1
            if next:
                self.f.write(',')
        if s_type == 1:
            self.perStudy(2) #PUT BACK, FOR TESTING ONLY
        self.conn.commit() #inserts done on db in above main loops

    def writeGP(self):
        logging.info('Writing germplasms ...')
        getgps = 'select * from GP'
        germs = self.__iterator(getgps)
        getbrapis = 'select * from GP_MULT where GERMPLASMNAME = ? and TYPE = ?'
        getstudy = 'select * from GP_MULT where GERMPLASMNAME = ? and TYPE = ?'
        gettax = getstudy
        for germ,isnext in germs:
            gp_dict = {'germplasmDbId':germ[0],'germplasmName':germ[1],'genus':germ[2],'species':germ[3],'subTaxa':None}
            brapis = self.__iterator(getbrapis,(gp_dict['germplasmDbId'],'BRAPI'))
            brapi_dict = {}
            for brapi,next in brapis:
                if brapi[1] in brapi_dict:
                    brapi_dict[brapi[1]].append(brapi[2])
                else:
                    brapi_dict[brapi[1]] = [brapi[2]] # add as list in case of multiple
            for key, value in brapi_dict.items():
                gp_dict[key] = ' / '.join(value)
            studies = self.__iterator(getstudy,(gp_dict['germplasmDbId'],'STUDY'))
            for study,next in studies:
                if study[1] in gp_dict:
                    gp_dict[study[1]].append(study[2])
                else:
                     gp_dict[study[1]] = [study[2]]
            taxes = self.__iterator(gettax,(gp_dict['germplasmDbId'],'TAX'))
            gp_dict['taxonIds'] = []
            for tax,next in taxes:
                gp_dict['taxonIds'].append({'sourceName':'ncbiTaxon', tax[1]:tax[2]})
            self.gpf.write(json.dumps(gp_dict,indent=4))
            if isnext:
                self.gpf.write(',')
#            print(json.dumps(gp_dict,indent=4))
            

    def __fillStudy(self,study,s_type = 1): #1 = get samples from sample table. 2 = get samples by joing data table
        curs = self.conn.cursor()
        query = 'select * from study where project = ?'
        pmt = (study.getPrj(),)
        curs.execute(query,pmt)
        row = curs.fetchone()
        study.addMeta('name',row[2]) #expecting 9 cols. check for this earlier in program, not need to check every row
        study.addMeta('title',row[3])
        study.addMeta('desc',row[4])
        study.addMeta('iso',row[5])
        study.addMeta('cult',row[6])
        study.addMeta('breed',row[7])
        study.addMeta('geo',row[8])
        if s_type == 1:
            query = 'select ID, TAX, STUDY, NAME, DESC, TITLE, CENTER, BROKER, AE from sample where study = ? or study = ?'
        else:
            query = 'select sample.ID, TAX, data.STUDY, sample.NAME, sample.DESC, sample.TITLE, sample.CENTER, sample.BROKER, sample.AE from sample join data on (sample.id = data.id) where sample.study is null and (data.study = ? or data.study = ?)'
        pmt = (study.getErp(),study.getPrj())
        samples = self.__iterator(query,pmt)
        runs=[]
        urlchecker = [] #so that duplicate urls are not added per study (possibility for analysis objects using mutliple samples)
        varlist = []
        reflist = []
        gplist = set()
        orgs = set()
        for row,next in samples:
            samp = Sample(row)
            self.__addRuns(samp)
            runs = runs + samp.getRunList() #should use .extend()
            self.__addVars(samp)
            self.__noUrlRepeat(samp.getVarList(), urlchecker, varlist)
            self.__addRefA(samp)
            self.__noUrlRepeat(samp.getRefList(), urlchecker, reflist)
            self.__findGermplasm(samp)
            if samp.getGermplasm():
                gplist.add(samp.getGermplasm())
                self.__registerGP(samp,study.getPrj())
            orgs.add(samp.getOrg())
        study.addRefGroup(self.__shrinkList(reflist))
        study.addVarGroup(self.__shrinkList(varlist))
        study.addRunGroup(self.__shrinkList(runs))
        study.addGpGroup(self.__shrinkList(list(gplist)))
        study.addOrgs(self.__shrinkList(list(orgs)))


    def __shrinkList(self, arr):
        max_size = 20 #hard coded
        return arr[:max_size]

    def __noUrlRepeat(self, newarr, urlcheck, cumarr):
        for d in newarr:
            if d['url'] not in urlcheck:
                cumarr.append(d)
                urlcheck.append(d['url'])
                
    def __iterator(self,query,pmt=()):
        curs = self.conn.cursor()
        curs.execute(query,pmt)
        row = curs.fetchone()
        while row is not None:
            row2 = curs.fetchone() #look ahead to see if it's the last one
            if row2 is not None:
                yield row, True
            else:
                yield row, False
            row = row2
        curs.close()

    def __registerGP(self,samp,study_id):
        rgp_curs = self.conn.cursor()
        rgp_insert = self.conn.cursor()
        insert_gp = 'insert into GP (GP_ID,GERMPLASMNAME,GENUS,SPECIES) values (?,?,?,?)'
        insert_gp_mult = 'insert into GP_MULT (GERMPLASMNAME, BFIELD, VALUE, TYPE) values (?,?,?,?)'
        germ = samp.getGermplasm()
        split_org = samp.getOrg().split()
        genus = split_org[0]
        species = " ".join(split_org[1:])
        try:
            rgp_insert.execute(insert_gp,(germ,germ,genus,species))
        except sqlite3.IntegrityError: 
            pass #germplasm already exists. but still add study
        try:
            rgp_insert.execute(insert_gp_mult,(germ,'studyDbId',study_id,'STUDY'))
        except sqlite3.IntegrityError: 
            pass #germplasm : study combo exists. but still add attributes
        try:
            rgp_insert.execute(insert_gp_mult,(germ,'taxonId',samp.getTax(),'TAX'))
        except sqlite3.IntegrityError: 
            pass #germplasm : tax id combo exists. but still add attributes
        find_command = 'select value from attributes where id = ? and field = ?'
        for i,att in enumerate(self.att_list):
            row = rgp_curs.execute(find_command,(samp.getId(),att)).fetchone()
            while row is not None: #should work for duplicated tags per sample (if and when the sample table supports them)
                try:
                    if self.brapi_eq:
                        rgp_insert.execute(insert_gp_mult,(germ,self.brapi_eq[i],row[0],'BRAPI'))
                    else:
                        rgp_insert.execute(insert_gp_mult,(germ,att,row[0],'NON-BRAPI'))
                except sqlite3.IntegrityError:
                    pass #for this gp and this field, value already registered. continue to next attribute
                row = rgp_curs.fetchone()

    def __addAtts(self,samp):
        att_curs = self.conn.cursor()
        command = 'select * from attributes where id = ?'
        att_curs.execute(command,(samp.getId(),))
        row = att_curs.fetchone()
        while row is not None:
            samp.addAtt(row[1],row[2],row[3])
            row = att_curs.fetchone()
        att_curs.close()

    def __addRuns(self,samp):
        run_curs = self.conn.cursor()
        command = 'select * from data where id = ? and type = ?'
        run_curs.execute(command,(samp.getId(),'ENA RUN'))
        row = run_curs.fetchone()
        while row is not None:
            samp.addRun(row[2],row[5]) #url, run id
            row = run_curs.fetchone()
        run_curs.close()

    def __addVars(self,samp): #add variation file
        var_curs = self.conn.cursor()
        command = 'select * from data where id = ? and type = ?'
        var_curs.execute(command,(samp.getId(),'SEQUENCE_VARIATION'))
        row = var_curs.fetchone()
        while row is not None:
            samp.addVar(row[2],row[5],row[6]) #url, analysis id, md5
            row = var_curs.fetchone()
        var_curs.close()      

    def __addRefA(self,samp): #add reference alignment files
        ref_curs = self.conn.cursor()
        command = 'select * from data where id = ? and type = ?'
        ref_curs.execute(command,(samp.getId(),'REFERENCE_ALIGNMENT'))
        row = ref_curs.fetchone()
        while row is not None:
            samp.addRefA(row[2],row[5],row[6]) #url, analysis id, md5
            row = ref_curs.fetchone()
        ref_curs.close() 

    def __findGermplasm(self,samp): #code repetition. should probably rewrite
        gp_curs = self.conn.cursor()
        command = Germplasm.makeCommand()
        terms = Germplasm.get_terms(samp.getId())
        gp_curs.execute(command,terms)
        rows = gp_curs.fetchall() #list of tuples
        for row in rows:
            gp = Germplasm.check(row[2]) 
            if gp is not None:
                samp.addGermplasm(gp) #first hit gets added. the rest get ignored
        command = Germplasm.makeSecond()
        terms = Germplasm.get_second(samp.getId())
        gp_curs.execute(command,terms)
        rows = gp_curs.fetchall() 
        for row in rows:
            gp = Germplasm.check(row[2])
            if gp is not None:
                samp.addGermplasm(gp)
        return None

    def close(self):
        self.conn.close()
        self.f.write(']')
        self.gpf.write(']')
        self.f.close()
        self.gpf.close()
    

class Study:


    def __init__(self,erp,prj):
        self.erp = erp
        self.prj = prj
        self.meta = {'name':None,'title':None,'desc':None,'iso':None,'cult':None,'breed':None,'geo':None}
        self.runs = []
        self.vars = []
        self.gps = [] #germplasm list
        self.orgs = []
        self.refs = []

    def addMeta(self,key,val):
        if key in self.meta:
            self.meta[key] = val

    def addRunGroup(self,arr):
        self.runs = arr

    def addVarGroup(self,arr):
        self.vars = arr

    def addGpGroup(self, arr):
        self.gps = arr

    def addOrgs(self, arr):
        self.orgs = arr

    def addRefGroup(self, arr):
        self.refs = arr
        

    def printMeta(self):
        for key,value in self.meta.items():
            if value:
                print('{} : {}'.format(key,value))

    def getErp(self):
        return self.erp

    def getPrj(self):
        return self.prj

    def printJ(self):
        d = {}
        d['studyTypeName'] = 'Genotyping' #late request
        d['studyDbId'] = self.prj
        d['organism'] = self.orgs
        d['documentationURL'] = 'https://www.ebi.ac.uk/ena/data/view/' + self.prj
        data = self.runs + self.vars + self.refs
        if len(data) > 0:
            d['dataLinks'] = data
        if len(self.gps) > 0:
            d['germplasmDbIds'] = self.gps
        if self.meta['title']:
            d['studyName'] = self.meta['title']
        elif self.meta['name']:
            d['studyName'] = self.meta['name']
        if self.meta['desc']:
            d['studyDescription'] = self.meta['desc']
            
        j = json.dumps(d,indent=4)
        return j


class Sample:


    def __init__(self, row):
        try:
            self.d = {'id': row[0],
                      'tax': row[1],
                      'study': row[2],
                      'name': row[3],
                      'desc': row[4],
                      'title': row[5],
                      'center': row[6],
                      'broker': row[7], #check for 'ArrayExpress' to find RNAseq data
                      'AE': row[8],
                      'attributes': {},
                      'runs': [],
                      'vars': [],
                      'alignments': []
                      }
        except IndexError as ie:
            logging.error('can not map sample table row to a dictionary {} {}'.format(row,ie))
            raise SystemExit
         
    def printJ(self):
        w = json.dumps(self.d,indent=4)
        return w

    def getId(self):
        return self.d['id']

    def addGermplasm(self,term):
        self.d['germplasm'] = term

    def getGermplasm(self):
        return self.d.get('germplasm', None)

    def getOrg(self):
        return self.d['name']

    def getTax(self):
        return self.d['tax']

    def addAtt(self,col,value,unit):
        valunit = value
        if unit is not None:
            valunit = val + ' ' + unit
        self.d['attributes'][col] = valunit

    def addRun(self,url,runid):
        self.d['runs'].append({'url': url, 'run id': runid})

    def getRunList(self):
        run_type = 'ENA genomic read files' if self.d['AE'] is None else 'ENA RNASeq read files'
        rl = [{'name': run['run id'], 'type': run_type, 'url': run['url']} for run in self.d['runs']]
        return rl

    def addVar(self,url,varid,md5):
        self.d['vars'].append({'url': url, 'var id': varid, 'md5': md5})

    def getVarList(self):
        var_type = 'genetic variation files'
        vl = [{'name':var['var id'], 'type':var_type, 'url':var['url']} for var in self.d['vars']]
        return vl

    def addRefA(self,url,refid,md5):
        self.d['alignments'].append({'url': url, 'refid': refid, 'md5': md5})

    def getRefList(self):
        ref_type = 'sequence read alignment files'
        rl = [{'name': ref['refid'], 'type': ref_type, 'url': ref['url'] } for ref in self.d['alignments']]
        return rl

    def setStudy(self,study_id):
        self.d['study'] = study_id

        
class Germplasm:
    
    liketerms = ('%germplasm%','%stock%','%accession%','specimen_voucher','specimen_voucher','culture_collection') 
    secondary = ('ecotype','cell_line','line','dna_designation','dna_accno_source','source material identifiers','source_mat_id','bio_material')

    command_f = 'select * from attributes where id = ? and ({})' 
    command_m = 'field like ? '

    @classmethod
    def get_terms(cls,samp):
        tup = (samp,) + cls.liketerms #change from test
        return tup

    @classmethod
    def get_second(cls,samp):
        tup = (samp,) + cls.secondary 
        return tup

    @classmethod
    def makeCommand(cls):
        command_p = cls.command_m 
        for term in range(len(cls.liketerms)-1):
            command_p = command_p + ' or ' + cls.command_m
        return cls.command_f.format(command_p)

    @classmethod
    def makeSecond(cls):
        command_p = cls.command_m 
        for term in range(len(cls.secondary)-1):
            command_p = command_p + ' or ' + cls.command_m
        return cls.command_f.format(command_p)
        
    def check(term):
        searchObj = re.search( r'([a-z0-9]+)([: _-]*)([a-z0-9]*)', term, re.I)
        if searchObj:
            first = searchObj.group(1)
            third = searchObj.group(3)
            label = searchObj.group()
            label_chars = len(re.findall('[a-zA-Z]', label))
            label_dig = len(re.findall('[0-9]', label))
            first_chars = len(re.findall('[a-zA-Z]', first))
            first_dig = len(re.findall('[0-9]', first))
            third_chars = len(re.findall('[a-zA-Z]', third))
            third_dig = len(re.findall('[0-9]', third))
         
            if label_chars < 2 or label_dig < 2:
                return None
            if label_chars < 3 and label_dig < 3:
                return None
            if third_dig == 1 and third_chars < 2: #filter off one number on its own 
                return None
            return label
        else:
            return None


def main():
    logging.basicConfig(filename=sys.argv[4], level=logging.DEBUG)
#    logging.basicConfig(level=logging.DEBUG) # print to standard out
    db = sys.argv[1]
    jsonf = sys.argv[2]
    jsongp = sys.argv[3]
    att_search = ['cultivar','biomaterial_provider','ref_biomaterial','geographic location (country and/or sea)','ecotype']
    brapi_equiv = ['subTaxa','instituteName','commonCropName','countryOfOriginCode','subTaxa'] 
    d = DumpSamples(db,jsonf,jsongp,att_search,brapi_equiv)
    d.perStudy() # or d.StudyList but NOT both
#    d.studyList(['SRP066162','SRP029353','ERP123456','DRP003562','DRP003767'],['ERP015076'])  
    d.writeGP()
    d.close()
    
if __name__== "__main__":
    main()
