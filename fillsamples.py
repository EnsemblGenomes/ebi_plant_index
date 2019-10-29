#!/nfs/production/panda/ensemblgenomes/development/mrossello/python_installation/bin/python

from lxml import etree
import ijson
import sqlite3
import sys
import logging

class FillSamples:

    def __init__(self,db_file,enasamp,analysis,enastud):
        self.context = etree.iterparse(enasamp,events=('end',),tag='SAMPLE')
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
        self.jfile = open(analysis,'rb')
        self.aobjects = ijson.items(self.jfile,'item')
        self.sjfile = open(enastud,'rb')
        self.sobjects = ijson.items(self.sjfile,'item')

        self.sample_num = 0
        self.current_sample = None

    def fillTables(self):
        for action, elem in self.context:
            self.sample_num += 1
            if self.sample_num % 1000 == 0:
                logging.info('parsing sample #{}'.format(str(self.sample_num)))

            try:
                self.__tableSample(elem)
            except AttributeError as err:
                logging.error('can not grab biosample_id/title/taxonomy for sample number {} so will not add to sample table: {}'.format(str(self.sample_num), err))
                continue
            except sqlite3.IntegrityError as ierr:
                logging.error('could not put sample {} into sample table. skipping. {}'.format(self.current_sample, ierr))
                continue
            except IdError as iderr:
                logging.error('for sample number {}, could not get id. Skipping. {}'.format(self.sample_num,iderr))
                continue

            try:
                self.__brokerName(elem) #add broker name and centre name
            except:
                logging.error("Unexpected error adding center/broker names to sample {}. {}".format(self.current_sample, sys.exc_info()))

            try:
                self.__tableAtt(elem)
            except:
                logging.error("Unexpected error adding attributes to sample:{}. {}".format(self.current_sample, sys.exc_info()))

            try:
                self.__tableLinks(elem)
            except:
                logging.error("Unexpected error adding ENA runs to sample:{}. {}".format(self.current_sample, sys.exc_info()))

            elem.clear()
# taken from here [https://www.ibm.com/developerworks/library/x-hiperfparse/index.html]. not sure if necesary but attempting to use less run time memory:
            while elem.getprevious() is not None: 
                del elem.getparent()[0]


    def __tableSample(self, elem): #<SAMPLE>
        desc = None
        taxname = None
        all_ext_tag = elem.find("IDENTIFIERS").findall("EXTERNAL_ID")
        ext_tag = [i.text for i in all_ext_tag if i.get('namespace') == "BioSample"]
        if len(ext_tag) < 1:
            raise IdError("can't find any BioSample in EXTERNAL_ID namespace")
        biosample_id = ext_tag[0]
        title = elem.find("TITLE").text
        taxnum = elem.find("SAMPLE_NAME").find("TAXON_ID").text
        taxname_element = elem.find("SAMPLE_NAME").find("SCIENTIFIC_NAME")
        if taxname_element is not None: # does happen. for eg SAMN00194153
            taxname = taxname_element.text
        else:
            taxname = elem.find("SAMPLE_NAME").find("COMMON_NAME").text
        desc_element = elem.find("DESCRIPTION")
        values = [biosample_id,taxnum,taxname,title]
        query = "INSERT INTO SAMPLE ({},{},{},{}) VALUES (?,?,?,?)".format("ID","TAX","NAME","TITLE")
        if desc_element is not None: #Description may or may not be available
            desc = desc_element.text
            query = "INSERT INTO SAMPLE ({},{},{},{},{}) VALUES (?,?,?,?,?)".format("ID","TAX","NAME","TITLE","DESC")
            values.append(desc)
        self.current_sample = biosample_id   
        self.conn.execute(query,values)
        self.conn.commit()

    def __tableAtt(self, elem): #<SAMPLE_ATTRIBUTES>
        attrs = elem.find("SAMPLE_ATTRIBUTES")
        for attr in attrs: 
            field = attr.find("TAG"); #not asking for .text so should not throw error
            value = attr.find("VALUE");
            units = attr.find("UNITS");
            if field is not None and value is not None:
                query = "INSERT INTO ATTRIBUTES (ID,FIELD,VALUE) VALUES (?,?,?)"
                values = [self.current_sample,field.text.lower(),value.text]
                if units is not None:
                    query = "INSERT INTO ATTRIBUTES (ID,FIELD,VALUE,UNITS) VALUES (?,?,?,?)"
                    values.append(units.text)
                try:
                    self.conn.execute(query,values)
                    self.conn.commit()
                except sqlite3.IntegrityError as inerr:
                    logging.error("integrity error adding attribute {}, value {}, to sample {}. The field has probably been used twice. Skipping this attribute. {}".format(field.text,value.text,self.current_sample, inerr))

    def __tableLinks(self,elem): #<SAMPLE_LINKS>
        links = elem.find("SAMPLE_LINKS")
        for link in links:
            tagdb = link[0][0].text
            if tagdb == "ENA-RUN":
                runs = link[0][1].text.split(',')
                exp_runs = []
                [exp_runs.extend(EnaAccTool.expandRange(dash)) for dash in runs]
                dir_urls = list(map(EnaAccTool.guessRunLink,exp_runs))
                for runid,url in zip(exp_runs,dir_urls):
                    query = "INSERT INTO DATA (ID,DATA_ID,TYPE,URL) VALUES (?,?,?,?)"
                    self.conn.execute(query,(self.current_sample,runid,'ENA RUN',url))
                    self.conn.commit()
            elif tagdb == "ENA-STUDY":
                studyid = link[0][1].text
                query = "UPDATE SAMPLE SET STUDY = ? WHERE ID = ?"
                self.conn.execute(query,(studyid,self.current_sample))
                self.conn.commit()
            elif tagdb == "ARRAYEXPRESS":
                aeid = link[0][1].text
                query = "UPDATE SAMPLE SET AE = ? WHERE ID = ?"
                self.conn.execute(query,(aeid,self.current_sample))
                self.conn.commit()

    def __brokerName(self, elem):
        broker = elem.get('broker_name') #look for 'ArrayExpress' to find RNAseq data
        center = elem.get('center_name')
        query = 'UPDATE SAMPLE SET CENTER = ?, BROKER = ? WHERE ID = ?'
        args = [center,broker,self.current_sample]
        self.conn.execute(query,args) #accepts null values
        self.conn.commit()
            
        
    def fillTableAnalysis(self):
#        for prefix, event, value in self.jparse:
#            print('prefix={}, event={}, value={}'.format(prefix, event, value))
        count = 0
        for o in self.aobjects: 
            anal_acc = o['analysis_accession']
            samp_acc = o['sample_accession']
            file_url = o['submitted_ftp']
            if file_url == '':
                logging.debug('skipping {} because no file found'.format(anal_acc))
                continue
            count += 1
            if count % 10000 == 0:
                logging.info('parsing analysis #{}'.format(str(count)))
            split_url = file_url.split(';')
            index = 0
            if len(split_url) == 2:
                if split_url[0].endswith('.md5'): #if not md5 then prob .cram/.vcf/.tab
                    index = 1
                file_url = split_url[index]
            md5 = o['submitted_md5'].split(';')[index] #tested with empty values and does not throw error
            study = o['study_accession']
            atype = o['analysis_type']
            title = o['analysis_title']
            query = "INSERT INTO DATA (ID,DATA_ID,TYPE,URL,STUDY,TITLE,MD5) VALUES (?,?,?,?,?,?,?)"
            try:
                self.conn.execute(query,(samp_acc,anal_acc,atype,file_url,study,title,md5)) #should catch error here so it doesn't give up at the first problem
                self.conn.commit()
            except sqlite3.IntegrityError as interr:
                logging.error('primary key unique constraint: data id {} and sample id {}. {}'.format(anal_acc,sample_acc,interr))
            except:
                logging.error("Unexpected error adding analysis {} / sample {}. count {}. {}:".format(anal_acc,samp_acc,str(count),sys.exc_info()[0]))


    def fillTableStudy(self):
        count = 0
        for o in self.sobjects:
            count +=1
            proj = o['study_accession']
            stud = o['secondary_study_accession']
            name = self.__indict(o,'study_name',self.__indict(o,'study_alias',None))
            title = self.__indict(o,'study_title',None)
            desc = self.__indict(o,'study_description',None)
            iso = self.__indict(o,'isolate',None)
            cult = self.__indict(o,'cultivar',None)
            breed = self.__indict(o,'breed',None)
            geo = self.__indict(o,'geo_accession',None)
            if count % 10000 == 0: #10000
                logging.info('parsing study #{} ({}/{})'.format(str(count),proj,stud))
            query = "INSERT INTO STUDY (PROJECT,STUDY,NAME,TITLE,DESC,ISOLATE,CULTIVAR,BREED,GEO_ACC) VALUES (?,?,?,?,?,?,?,?,?)"
            try:
                self.conn.execute(query,(proj,stud,name,title,desc,iso,cult,breed,geo))
                self.conn.commit()
            except sqlite3.IntegrityError as interr:
                logging.error('primary key unique constraint: project id {} and study id {}. {}'.format(proj,stud,interr))                
            except:
                logging.error("Unexpected error adding project {} / study {}. count {}. {}:".format(proj,study,str(count),sys.exc_info()[0]))
            

    def __indict(self,d,k,default): #helper function: if key in dict return value else return default
        if k in d:
            return d[k]
        else:
            return default      


    def close(self):
        del self.context
        self.cur.close()
        self.conn.close()
        self.jfile.close()
        self.sjfile.close()


class EnaAccTool:

    @staticmethod
    def expandRange(dash): 
        arr = dash.split('-')
        if len(arr) > 1:
            pref = arr[0][:3]
            expanded = list(range(int(arr[0][3:]),int(arr[1][3:])+1))
            needzero = len(arr[1][3:]) - len(str(int(arr[1][3:]))) #flanking 0s dissappear when doing int conversion, so i need to put them back
            if needzero > 0:
                pref += ('0' * needzero)
            return [pref + str(x) for x in expanded]
        else:
            return [dash]

    @staticmethod
    def guessRunLink(runid):
        #SRR7849281 = ftp.sra.ebi.ac.uk/vol1/fastq/SRR784/001/SRR7849281/SRR7849281_1.fastq.gz
        #And one for _2 file too. can't know if paired so will guess directory only
        baseurl = 'ftp://ftp.sra.ebi.ac.uk/vol1/fastq/'
        pref = runid[:6]
        sev = '00' + runid[-1]
        fullurl = baseurl + pref + '/' + runid
        if len(runid) > 9:
            fullurl = baseurl + pref + '/' + sev + '/' + runid
        return fullurl

class IdError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def main():
    logging.basicConfig(filename=sys.argv[5], level=logging.DEBUG)
#    logging.basicConfig(level=logging.DEBUG) #to stdout
    db = sys.argv[1]
    enasamp = sys.argv[2]
    analysis = sys.argv[3]
    enastud = sys.argv[4]
    s = FillSamples(db,enasamp,analysis,enastud)
    s.fillTables()
    s.fillTableAnalysis()
    s.fillTableStudy()
    s.close()

if __name__== "__main__":
    main()
