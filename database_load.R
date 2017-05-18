library(scidb)
DB = scidbconnect()
WORK_DIR = '/home/scidb/campaign_finance'
setwd(WORK_DIR)

recreate_db = function()
{
  confirm = readline(prompt = "This will erase the DB, are you sure? (y/n): ")
  if(confirm != 'y' && confirm != 'Y')
  {
    print("Exiting")
    return();
  }
  for( i in 1:length(ARRAYS))
  {
    name = names(ARRAYS)[i]
    schema = ARRAYS[[i]]
    tryCatch({ iquery(DB, paste0("remove(", name, ")"))  },  warning = invisible, error = invisible )
    iquery(DB, paste0("create array ", name, " ", schema))
  }
  tryCatch({ iquery(DB, "remove(ENTITY)")},  warning = invisible, error = invisible )
  tryCatch({ iquery(DB, "remove(TRANSACTION)")},  warning = invisible, error = invisible )
}

ARRAYS = list()
ARRAYS["COMMITTEE"] = "
                  < committee_id            : string,
                    committee_name          : string,
                    committee_treasurer_name: string,
                    committee_street_1      : string,
                    committee_street_2      : string,
                    committee_city          : string,
                    committee_state         : string,
                    committee_zip           : string,
                    committee_designation   : char,
                    committee_type          : char,
                    committee_party         : string,
                    committee_filing_freq   : char,
                    committee_org_type      : char,
                    committee_connected_org : string,
                    committee_candidate_id  : string>
                  [committee_idx = 0:*]"


load_committee = function()
{
  system("wget ftp://ftp.fec.gov/FEC/2016/cm16.zip", ignore.stdout=T, ignore.stderr=T)
  system("unzip cm16.zip")
  iquery(DB, sprintf("
   store(
     project(
      unpack(
       apply(
        aio_input(
          '%s/cm.txt', 'num_attributes=15', 'attribute_delimiter=|'
        ),
        committee_id,                a0, 
        committee_name,              a1,
        committee_treasurer_name,    a2,
        committee_street_1,          a3,
        committee_street_2,          a4,
        committee_city,              a5,
        committee_state,             a6,
        committee_zip,               a7,
        committee_designation,       char(a8),
        committee_type,              char(a9),
        committee_party,             a10,
        committee_filing_freq,       char(a11),
        committee_org_type,          char(a12),
        committee_connected_org,     a13,
        committee_candidate_id,      a14
       ),
       committee_idx
      ),
      committee_id, committee_name, committee_treasurer_name, committee_street_1,
      committee_street_2, committee_city, committee_state, committee_zip, committee_designation,
      committee_type,  committee_party,  committee_filing_freq, committee_org_type, committee_connected_org, 
      committee_candidate_id
     ),
     COMMITTEE
   )", WORK_DIR))
  print(iquery(DB, "op_count(COMMITTEE)", return=T))
  print(system("wc -l cm.txt", intern=T))
  system("rm cm.txt")
  system("rm cm16.zip")
}

ARRAYS["CANDIDATE"] = "
                  < candidate_id            : string,
                    candidate_name          : string,
                    candidate_party         : string,
                    candidate_election_year : int32,
                    candidate_office_state  : string,
                    candidate_office        : char,
                    candidate_office_dist   : string,
                    candidate_ici           : char,
                    candidate_status        : char,
                    candidate_pcc           : string,
                    candidate_street1       : string,
                    candidate_street2       : string,
                    candidate_city          : string,
                    candidate_state         : string,
                    candidate_zip           : string>
                  [candidate_idx = 0:*]"

load_candidate = function()
{
  system("wget ftp://ftp.fec.gov/FEC/2016/cn16.zip", ignore.stdout=T, ignore.stderr=T)
  system("unzip cn16.zip")
  iquery(DB, sprintf("
   store(
     project(
      unpack(
       apply(
        aio_input(
          '%s/cn.txt', 'num_attributes=15', 'attribute_delimiter=|'
        ),
        candidate_id,                a0, 
        candidate_name,              a1,
        candidate_party,             a2,
        candidate_election_year,     dcast(a3, int32(null)),
        candidate_office_state,      a4,
        candidate_office,            char(a5),
        candidate_office_dist,       a6,
        candidate_ici,               char(a7),
        candidate_status,            char(a8),
        candidate_pcc,               a9,
        candidate_street1,           a10,
        candidate_street2,           a11,
        candidate_city,              a12,
        candidate_state,             a13,
        candidate_zip,               a14
       ),
       candidate_idx
      ),
      candidate_id, candidate_name, candidate_party, candidate_election_year,
      candidate_office_state, candidate_office, candidate_office_dist, candidate_ici, candidate_status,
      candidate_pcc,  candidate_street1,  candidate_street2, candidate_city, candidate_state, 
      candidate_zip
     ),
     CANDIDATE
   )", WORK_DIR))
  print(iquery(DB, "op_count(CANDIDATE)", return=T))
  print(system("wc -l cn.txt", intern=T))
  system("rm cn.txt")
  system("rm cn16.zip")
}

ARRAYS["CANDIDATE_COMMITTEE_LINKAGE"] = "
                  < candidate_id            : string,
                    candidate_election_year : int32,
                    fec_election_year       : int32,
                    committee_id            : string,
                    committee_type          : char,
                    committee_designation   : char,
                    linkage_id              : int64>
                  [ccl_idx = 0:*]"

load_ccl = function()
{
  system("wget ftp://ftp.fec.gov/FEC/2016/ccl16.zip", ignore.stdout=T, ignore.stderr=T)
  system("unzip ccl16.zip")
  iquery(DB, sprintf("
   store(
     project(
      unpack(
       apply(
        aio_input(
          '%s/ccl.txt', 'num_attributes=7', 'attribute_delimiter=|'
        ),
        candidate_id,                a0, 
        candidate_election_year,     dcast(a1, int32(null)),
        fec_election_year,           dcast(a2, int32(null)),
        committee_id,                a3,
        committee_type,              char(a4),
        committee_designation,       char(a5),
        linkage_id,                  int64(a6)
       ),
       ccl_idx
      ),
      candidate_id, candidate_election_year, fec_election_year, committee_id, committee_type, 
      committee_designation, linkage_id
     ),
     CANDIDATE_COMMITTEE_LINKAGE
   )", WORK_DIR))
  print(iquery(DB, "op_count(CANDIDATE_COMMITTEE_LINKAGE)", return=T))
  print(system("wc -l ccl.txt", intern=T))
  system("rm ccl.txt")
  system("rm ccl16.zip")
}

ARRAYS["COMMITTEE_TO_COMMITTEE"] = "
                  < committee_id            : string,
                    amendment_id            : char,
                    report_type             : string,
                    transaction_pgi         : string,
                    image_num               : string,
                    transaction_type        : string,
                    entity_type             : string,
                    name                    : string,
                    city                    : string,
                    state                   : string,
                    zip                     : string,
                    employer                : string, 
                    occupation              : string,
                    transaction_date_int    : int64,
                    transaction_amount      : double,
                    other_id                : string, 
                    transaction_id          : string,
                    file_num                : int64,
                    memo_co                 : char,
                    memo_text               : string,
                    sub_id                  : string>
                  [oth_idx = 0:*]"

load_oth = function()
{
 system("wget ftp://ftp.fec.gov/FEC/2016/oth16.zip", ignore.stdout=T, ignore.stderr=T)
 system("unzip oth16.zip")
 iquery(DB, sprintf("
  store(
    project(
     unpack(
      apply(
       aio_input(
         '%s/itoth.txt', 'num_attributes=21', 'attribute_delimiter=|'
       ),
       committee_id,                a0, 
       amendment_id,                char(a1),
       report_type,                 a2,
       transaction_pgi,             a3,
       image_num,                   a4,
       transaction_type,            a5,
       entity_type,                 a6,
       name,                        a7,
       city,                        a8,
       state,                       a9, 
       zip,                         a10,
       employer,                    a11,
       occupation,                  a12,
       transaction_date_int,        iif(strlen(a13) > 0, 
                                        int64(substr(a13, 4,4)) * 10000 +  int64(substr(a13, 0,2))* 100 + int64(substr(a13, 2,2)),
                                        int64(null)),
       transaction_amount,          double(a14),
       other_id,                    iif(a15 = '', null, a15),
       transaction_id,              a16,
       file_num,                    dcast(a17, int64(null)),
       memo_co,                     char(a18),
       memo_text,                   a19,
       sub_id,                      a20
      ),
      oth_idx
     ),
     committee_id, amendment_id, report_type, transaction_pgi, image_num, transaction_type, 
     entity_type, name, city, state, zip, employer, occupation, transaction_date_int, transaction_amount, 
     other_id, transaction_id, file_num, memo_co, memo_text, sub_id
    ),
    COMMITTEE_TO_COMMITTEE
   )", WORK_DIR))
  print(iquery(DB, "op_count(COMMITTEE_TO_COMMITTEE)", return=T))
  print(system("wc -l itoth.txt", intern=T))
  system("rm itoth.txt")
  system("rm oth16.zip")
}

ARRAYS["INDIVIDUAL_CONTRIBUTIONS"] = "
                  < committee_id            : string,
                    amendment_id            : char,
                    report_type             : string,
                    transaction_pgi         : string,
                    image_num               : string,
                    transaction_type        : string,
                    entity_type             : string,
                    name                    : string,
                    city                    : string,
                    state                   : string,
                    zip                     : string,
                    employer                : string, 
                    occupation              : string,
                    transaction_date_int    : int64,
                    transaction_amount      : double,
                    other_id                : string, 
                    transaction_id          : string,
                    file_num                : int64,
                    memo_co                 : char,
                    memo_text               : string,
                    sub_id                  : string>
                  [indiv_idx = 0:*]"

load_indiv = function()
{
 system("wget ftp://ftp.fec.gov/FEC/2016/indiv16.zip", ignore.stdout=T, ignore.stderr=T)
 system("unzip indiv16.zip")
 #The zip archive expands into two copies: one is a "by_date" directory
 #and the other is a regular text file with all the data.
 #Both appear to contain the same information. :/ Why?
 #Surely someone was trying to be nice...
 iquery(DB, sprintf("
  store(
    project(
     unpack(
      apply(
       aio_input(
         '%s/itcont.txt', 'num_attributes=21', 'attribute_delimiter=|'
       ),
       committee_id,                a0, 
       amendment_id,                char(a1),
       report_type,                 a2,
       transaction_pgi,             a3,
       image_num,                   a4,
       transaction_type,            a5,
       entity_type,                 a6,
       name,                        a7,
       city,                        a8,
       state,                       a9, 
       zip,                         a10,
       employer,                    a11,
       occupation,                  a12,
       transaction_date_int,        iif(strlen(a13) > 0, 
                                        int64(substr(a13, 4,4)) * 10000 +  int64(substr(a13, 0,2))* 100 + int64(substr(a13, 2,2)),
                                        int64(null)),
       transaction_amount,          double(a14),
       other_id,                    iif(a15 = '', null, a15),
       transaction_id,              a16,
       file_num,                    dcast(a17, int64(null)),
       memo_co,                     char(a18),
       memo_text,                   a19,
       sub_id,                      a20
      ),
      indiv_idx
     ),
     committee_id, amendment_id, report_type, transaction_pgi, image_num, transaction_type, 
     entity_type, name, city, state, zip, employer, occupation, transaction_date_int, transaction_amount, 
     other_id, transaction_id, file_num, memo_co, memo_text, sub_id
    ),
    INDIVIDUAL_CONTRIBUTIONS
   )", WORK_DIR))
  print(iquery(DB, "op_count(INDIVIDUAL_CONTRIBUTIONS)", return=T))
  print(system("wc -l itcont.txt", intern=T))
  system("rm itcont.txt")
  system("rm indiv16.zip")
  system("rm -rf by_date")
}

create_entity = function()
{
  tryCatch({ iquery(DB, "remove(ENTITY_TMP)")  },  warning = invisible, error = invisible )
  iquery(DB, "create temp array ENTITY_TMP  
    < entity_id            : string,
      entity_type          : int64,   --1: candidate, 2: committee, 3: individual, 4: unknown
      entity_name          : string>
    [entity_idx = 0:*,400000,0]")
  
  #Add all the candidates 
  iquery(DB, "
   insert(
     project(
      unpack(
       apply(
        CANDIDATE,
        entity_id, candidate_id,
        entity_type, 1, 
        entity_name, candidate_name
       ),
       entity_idx, 100000
      ),
      entity_id, entity_type, entity_name
     ),
    ENTITY_TMP
   )")
    
  #Add the committees
  iquery(DB, "
   insert(
     redimension(
       apply(
        cross_join(
          COMMITTEE,
          aggregate(apply(ENTITY_TMP, ival, entity_idx), max(ival) as max_idx)
        ),
        entity_id, committee_id,
        entity_type, 2, 
        entity_name, committee_name,
        entity_idx, max_idx + 1 + committee_idx
       ),
       ENTITY_TMP
      ),
    ENTITY_TMP
   )")
  
  #Add the unique individual contributors
  iquery(DB, "
   insert(
    redimension(
     apply(
      cross_join(
       unpack(
        grouped_aggregate( 
         apply(
          project(
            INDIVIDUAL_CONTRIBUTIONS,
           name, zip, employer, occupation
          ),
          entity_id, 'I:' + name + '|' + zip + '|' + employer + '|' + occupation,
          entity_type, 3, 
          entity_name, name
         ),
         count(*), entity_id, entity_type, entity_name
        ),
        unique_individual_idx
       ),
       aggregate(apply(ENTITY_TMP, ival, entity_idx), max(ival) as max_idx)
      ),
      entity_idx, max_idx + 1 + unique_individual_idx
     ),
     ENTITY_TMP
    ),
    ENTITY_TMP
   )"
  )
  
  iquery(DB, "
   insert(
    redimension(
     apply(
      cross_join(
       unpack(
        filter(
         equi_join(
          grouped_aggregate( 
           apply(
            project(
             COMMITTEE_TO_COMMITTEE,
             name, zip, employer, occupation
            ),
            entity_id, 'I:' + name + '|' + zip + '|' + employer + '|' + occupation,
            entity_type, 3, 
            entity_name, name
           ),
           count(*), entity_id, entity_type, entity_name
          ) as A,
          apply(
           project(ENTITY_TMP, entity_id),
           z, 1
          ),
          'left_names=entity_id',
          'right_names=entity_id',
          'left_outer=T'
         ),
         z is null
        ),
        unique_individual_idx
       ),
       aggregate(apply(ENTITY_TMP, ival, entity_idx), max(ival) as max_idx)
      ),
      entity_idx, max_idx + 1 + unique_individual_idx
     ),
     ENTITY_TMP
    ),
    ENTITY_TMP
   )"
  )
  
  iquery(DB, "
   insert(
    redimension(
     apply(
      cross_join(
       unpack(
        apply(
         grouped_aggregate(
          filter(
           equi_join(
            project(filter(COMMITTEE_TO_COMMITTEE, other_id is not null), other_id, name, zip),
            project(ENTITY_TMP, entity_id, entity_name),
            'left_names=other_id',
            'right_names=entity_id',
            'left_outer=1'
           ),
           entity_name is null
          ),
          count(*), max(name) as name, max(zip) as zip, other_id
         ),
         entity_id, other_id, 
         entity_name, 'Unlisted Entity:' + name  + '|' + zip,
         entity_type, 4
        ),
        unique_individual_idx
       ),
       aggregate(apply(ENTITY_TMP, ival, entity_idx), max(ival) as max_idx)
      ),
      entity_idx, max_idx + 1 + unique_individual_idx
     ),
     ENTITY_TMP
    ),
    ENTITY_TMP
   )")
  
  iquery(DB, "
   insert(
    redimension(
     apply(
      cross_join(
       unpack(
        apply(
         grouped_aggregate(
          filter(
           equi_join(
            project(filter(INDIVIDUAL_CONTRIBUTIONS, other_id is not null), other_id, name, zip),
            project(ENTITY_TMP, entity_id, entity_name),
            'left_names=other_id',
            'right_names=entity_id',
            'left_outer=1'
           ),
           entity_name is null
          ),
          count(*), max(name) as name, max(zip) as zip, other_id
         ),
         entity_id, other_id, 
         entity_name, 'Unlisted Entity:' + name  + '|' + zip,
         entity_type, 4
        ),
        unique_individual_idx
       ),
       aggregate(apply(ENTITY_TMP, ival, entity_idx), max(ival) as max_idx)
      ),
      entity_idx, max_idx + 1 + unique_individual_idx
     ),
     ENTITY_TMP
    ),
    ENTITY_TMP
   )")

  #In the Committee_to_committee dataset, there was also one "TO" committee id that was not in the COMMITTEE list. Whoops!
  iquery(DB, "
   insert(
    redimension(
     apply(
      cross_join(
       unpack(
        apply(
         grouped_aggregate(
          filter(
           equi_join(
            project(COMMITTEE_TO_COMMITTEE, committee_id),
            project(ENTITY_TMP, entity_id, entity_name),
            'left_names=committee_id',
            'right_names=entity_id',
            'left_outer=1'
           ),
           entity_name is null
          ),
          count(*), committee_id
         ),
         entity_id, committee_id, 
         entity_name, 'Unlisted Entity',
         entity_type, 4
        ),
        unique_individual_idx
       ),
       aggregate(apply(ENTITY_TMP, ival, entity_idx), max(ival) as max_idx)
      ),
      entity_idx, max_idx + 1 + unique_individual_idx
     ),
     ENTITY_TMP
    ),
    ENTITY_TMP
   )")
  
  cnt = iquery(DB, "op_count(filter(grouped_aggregate(ENTITY_TMP, count(*), entity_id), count>1))",return=T)
  if(cnt$count != 0) 
  {
    stop("Whoops! Got duplicate entities")
  }
  
  num_entities= iquery(DB, "op_count(ENTITY_TMP)", return=T)$count
  iquery(DB, sprintf("create array ENTITY
    < entity_id            : string,
      entity_type          : int64,   --1: candidate, 2: committee, 3: individual, 4: unknown
      entity_name          : string>
    [entity_idx = 0:%i,400000,0]", num_entities-1))
  
  #Now shuffle the array and store
  iquery(DB, "store(project(sort(apply(ENTITY_TMP, r, random()), r, 100000), entity_id, entity_type, entity_name), ENTITY)")
  tryCatch({ iquery(DB, "remove(ENTITY_TMP)")  },  warning = invisible, error = invisible )
}

create_transaction = function()
{
  tryCatch({ iquery(DB, "remove(TRANSACTIONS_TMP)")  },  warning = invisible, error = invisible )
  iquery(DB, "create array TRANSACTIONS_TMP 
             <transaction_amount:double, transaction_type:string, transaction_id:string, transaction_date_int:int64, from_entity_idx:int64, to_entity_idx:int64>
             [transaction_idx=0:*,100000,0]")  
  
  iquery(DB, "
   insert(
     project(
      unpack(
       project(
        equi_join(
         equi_join(
          project(
           apply(
            apply(
             COMMITTEE_TO_COMMITTEE,
             individual_id,  'I:' + name + '|' + zip + '|' + employer + '|' + occupation
            ),
            from_entity_id,  iif(other_id is not null and entity_type<>'IND', other_id, individual_id)
           ),
           committee_id, from_entity_id, transaction_amount, transaction_type, transaction_id, transaction_date_int
          ),
          apply(project(ENTITY, entity_id), from_entity_idx, entity_idx), 
          'left_names=from_entity_id', 
          'right_names=entity_id'
         ),
         apply(project(ENTITY, entity_id), to_entity_idx, entity_idx),       
         'left_names=committee_id',
         'right_names=entity_id'
        ),
        transaction_amount, transaction_type, transaction_id, transaction_date_int, from_entity_idx, to_entity_idx
       ),
       transaction_idx, 100000
      ),
      transaction_amount, transaction_type, transaction_id, transaction_date_int, from_entity_idx, to_entity_idx
     ),
     TRANSACTIONS_TMP
    )"
   )
  
  iquery(DB, "
   insert(
    redimension( 
     apply(
      cross_join(
       project(
        unpack(
         project(
          equi_join(
           equi_join(
            project(
             apply(
              apply(
               INDIVIDUAL_CONTRIBUTIONS,
               individual_id,  'I:' + name + '|' + zip + '|' + employer + '|' + occupation
              ),
              from_entity_id,  iif(other_id is not null and entity_type<>'IND', other_id, individual_id)
             ),
             committee_id, from_entity_id, transaction_amount, transaction_type, transaction_id, transaction_date_int
            ),
            apply(project(ENTITY, entity_id), from_entity_idx, entity_idx), 
            'left_names=from_entity_id', 
            'right_names=entity_id'
           ),
           apply(project(ENTITY, entity_id), to_entity_idx, entity_idx),       
           'left_names=committee_id',
           'right_names=entity_id'
          ),
          transaction_amount, transaction_type, transaction_id, transaction_date_int, from_entity_idx, to_entity_idx
         ),
         j, 100000
        ),
        transaction_amount, transaction_type, transaction_id, transaction_date_int, from_entity_idx, to_entity_idx
       ),
       aggregate(apply(TRANSACTIONS_TMP, tidx_val, transaction_idx), max(tidx_val) as max_idx)
      ),
      transaction_idx, max_idx + j + 1
     ),
     TRANSACTIONS_TMP
    ),
    TRANSACTIONS_TMP
   )"
  )
  num_transactions = iquery(DB, "op_count(TRANSACTIONS_TMP)",return=T)
  num_c2c = iquery(DB, "op_count(COMMITTEE_TO_COMMITTEE)",return=T)
  num_ind = iquery(DB, "op_count(INDIVIDUAL_CONTRIBUTIONS)",return=T)
  if(num_transactions$count != num_c2c$count + num_ind$count)
  {
    stop("Some transactions are missing!")
  }
  num_entities= iquery(DB, "op_count(ENTITY)", return=T)$count
  iquery(DB, sprintf(" create array TRANSACTION
    < transaction_amount: double,
      transaction_id: string, 
      transaction_type: string>
    [from_entity_idx = 0:%i,400000,0,
     to_entity_idx   = 0:%i,400000,0,
     transaction_date_int = 0:*,10000,0,
     synthetic       = 0:*,5000,0]", num_entities-1, num_entities-1))
  iquery(DB, "store(redimension(TRANSACTIONS_TMP, TRANSACTION), TRANSACTION)")
  tryCatch({ iquery(DB, "remove(TRANSACTIONS_TMP)")  },  warning = invisible, error = invisible )
}

load_all = function()
{
  recreate_db()
  print("Loading committees")
  load_committee()
  print("Loading candidates")
  load_candidate()
  print("Loading candidate-committee-linkage")
  load_ccl()
  print("Loading committee-committee transfers")
  load_oth()
  print("Loading individual-committee transfers")
  load_indiv()
  print("Extracting unique entities")
  create_entity()
  print("Clustering entity transactions")
  create_transaction()
}
  

