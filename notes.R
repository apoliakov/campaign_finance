
COMMITTEE = scidb(DB, "COMMITTEE")
as.R(DB$project(DB$filter(COMMITTEE, "regex(committee_name, \'.*TRUMP.*\')" ), committee_name, committee_city, committee_type, committee_state, committee_candidate_id))
as.R(DB$project(DB$filter(COMMITTEE, "regex(committee_name, \'.*JEB.*\')" ), committee_name, committee_city, committee_state, committee_candidate_id))


r = as.R(DB$sort(DB$grouped_aggregate(DB$equi_join( DB$filter(CANDIDATE, "candidate_office='P'"), COMMITTEE, "'left_names=candidate_id'", "'right_names=committee_candidate_id'"), "count(*)", candidate_id, candidate_name), 
   "count desc"))

candidates = DB$project(DB$filter(CANDIDATE, "candidate_office='P'"), candidate_id, candidate_name)
committees = DB$project(COMMITTEE, committee_id, committee_candidate_id)
ccl1 = DB$equi_join(candidates, committees, "'left_names=candidate_id'", "'right_names=committee_candidate_id'", "'left_outer=T'")
ccl2 = DB$equi_join(candidates, DB$project(CCL, candidate_id, committee_id), "'left_names=candidate_id'", "'right_names=candidate_id'", "'left_outer=T'")
ccl_total = DB$equi_join(ccl1, ccl2, "'left_names=candidate_id,committee_id'", "'right_names=candidate_id,committee_id'", "'left_outer=T'", "'right_outer=T'")
ccl_total = scidb::store(DB, ccl_total)
as.R(DB$filter(ccl_total, "regex(candidate_name, '.*TRUMP.*')"))

#Trump Committees
as.R(DB$filter(CCL, "candidate_id='P80001571'"))
as.R(DB$filter(COMMITTEE, "committee_candidate_id='P80001571'"))

#Hillary Committees
as.R(DB$filter(CCL, "candidate_id='P00003392'"))
as.R(DB$filter(COMMITTEE, "committee_candidate_id='P00003392'"))

#Bernie
as.R(DB$filter(CCL, "candidate_id='P60007168'"))
as.R(DB$filter(COMMITTEE, "committee_candidate_id='P60007168'"))

#89 grand in individual contributions unnacounted for, say what?
iquery(DB, "grouped_aggregate(filter(INDIVIDUAL_CONTRIBUTIONS, name='' and zip='' and employer='' and occupation=''), sum(transaction_amount), transaction_type)",return=T)
#But it doesn't appear to be going to interesting candidates
iquery(DB, "equi_join(grouped_aggregate(filter(INDIVIDUAL_CONTRIBUTIONS, name='' and zip='' and employer='' and occupation=''), sum(transaction_amount), committee_id), ENTITY, 'left_names=committee_id', 'right_names=entity_id')",return=T)

#All donations by 
iquery(DB, "equi_join(ENTITY, grouped_aggregate(filter(INDIVIDUAL_CONTRIBUTIONS, regex(name, '.*TRUMP.*') and regex(name, '.*DONALD.*')), sum(transaction_amount), name, zip, transaction_type, committee_id), 'left_names=entity_id', 'right_names=committee_id')", return=T)

