# Collation of EBI Plant Samples and associated data files into JSON for incorporation into FAIR Data-finder for Agronomic REsearch

### Introduction

As part of Elixir Work Package 7, Ensembl Plants have produced this project to collate EBI plant data into 2 JSON files to be incorporated into FAIDARE: "FAIR Data-finder for Agronomic REsearch" (https://urgi.versailles.inra.fr/faidare/). The participants of work package 7 are crop plant research institutes who implement the BrAPI breeding API on top of their crop experiment databases. FAIDARE regularly makes specific BrAPI calls to each member API to collect details of available germplasms and phenotyping studies and indexes these as part of the federated search so that crop experiments and metadata can be discovered from a single centralised point.

The EBI has many databases and services so it is not feasible to build a custom plant API on top of them. However EBI is an important resource for genetic experiments including NGS sequencing, genotyping/variant calling, reference assemblies and expression studies (RNA-Seq). Specialist EBI groups in these data types exist such as EVA, Expression Atlas and BioSamples but all the data files are archived at the ENA, who have powerful search and API services. So in this project ENA is used to find all plant data across the different services at the EBI and 2 JSON files are created. One, called gp_dump.json contains all the germplasms found. It corresponds to brapi/v1/germplasms BrAPI call. The second one is study_dump.json and this contains all studies found and corresponds to BrAPI call brapi/v1/studies. Each study has a list of germplasms used and each germplasm has a list of studies it belongs to so the files cross reference each other. 

The JSON files are placed on an ftp server so that FAIRDARE can access them. The contacts at FAIRDARE arranging this are:
Cyril Pommier <cyril.pommier@inra.fr>
Bilal El-Houdaigui <Bilal.El-Houdaigui@inra.fr>
Jeremy Destin <Jeremy.Destin@inra.fr>

### Customisations

ENA and Biosamples do not impose a mandatory germplasm field for plant sample submissions but the germplasm id is central to crop phenotyping experiments. To create the germplasm JSON each EBI sample is checked for a germplasm id using a regular expression on its main id fields. If found, a germplasm is created by extracting the guessed germplasm id from the sample metadata along with other (potentially informative) attributes. You can also pull other metadata fields into the germplasm objects and you can provide MIAPPE terms for them. For instance if and when the ENA sample as 'cultivar' annotated you can pull it into the germplasm object under MIAPPE term 'subTaxa'. To do this edit 2 list variable in dumpsamples.py: att_search and brapi_equiv. For example:
```python
att_search = ['cultivar','biomaterial_provider','ref_biomaterial','geographic location (country and/or sea)','ecotype']
brapi_equiv = ['subTaxa','instituteName','commonCropName','countryOfOriginCode','subTaxa'] 
```
In the above example both cultivar and ecotype will get put into MIAPPE 'subTaxa' field, so it is simply done by order of occurence in both lists. This feature is useful because you will find common fields being annotated in the ENA samples which you can use the EbiSample_content.db sqlite3 database which will appear after the main shell script is run. Look at the SAMPLE table and join with ATTRIBUTES table SAMPLE.ID = ATTRIBUTES.ID. Alternatively, instead of looking for common attributes in ENA samples you may choose to look for attributes that have been agreed in the community or that are form a specific ENA checklist (https://www.ebi.ac.uk/ena/submit/checklists).

### Python packages
Version used: Python 3.5.1

#### packages used
* certifi==2018.4.16
* chardet==3.0.4
* idna==2.7
* ijson==2.3
* lxml==4.2.1
* requests==2.19.1
* urllib3==1.23

You do not have to install any packages if you use the original Python installation (EBI internal) for this project here:
/nfs/production/panda/ensemblgenomes/development/mrossello/python_installation/bin/python

Both Python scripts fillsamples.py and dumpsamples.py have the above installation in their 'shebang' line and thus can be run from the command line or via the shell script included without any additional action. 

For testing or running independently you can enter a virtual environment by executing the following
```bash
source /nfs/production/panda/ensemblgenomes/development/mrossello/python_installation/bin/activate
```

### Set up variables
All variables and settings are in the bash shell script all_down.sh.
main_dir defaults to the repo directory as downloaded. This should be fine and will be used s the location for the next few veriables including the log files.
Variable 'ftp_dir' is the location of the ftp server where the JSONs will be dumped. This is already selected and communicated to our FAIRDARE colleagues but ofcourse can be changed if necessary. 
The last view customiseable variables are for temporary files and these are deleted at run time so it is not necessary to change these.

### Execute the code

To execute the code run the bash shell script all_down.sh. You can send it to the EBI LSF cluster or set up a cron job with it. Do not run it on any the shared nodes (ebi-cli for example) because it takes a while. Last run for example took took 1 days 6 hours and 24 minutes.
```bash
bsub all_down.sh
```


