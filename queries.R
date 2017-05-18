library(scidb)
DB = scidbconnect()

ENTITY = scidb(DB, "ENTITY")
TRANSACTION = scidb(DB, "TRANSACTION")
CCL = scidb(DB, "CCL")

library('xts')
library('dygraphs')

entity_lookup = function(regex)
{
  filter_expr = sprintf("regex(entity_name, '.*%s.*')", regex)
  as.R(DB$filter(ENTITY, R(filter_expr)))
}

plot_candidate_donations = function(candidate_regex='TRUMP, DONALD', entity_id=NULL)
{
  if(!is.null(entity_id))
  {
    filter_expr = sprintf("entity_id='%s'", entity_id)
  } else {
    filter_expr = sprintf("entity_type=1 and regex(entity_name, '.*%s.*')", candidate_regex)
  }
  candidate_entity = DB$filter(ENTITY, R(filter_expr))
  candidate_entity_r = as.R(candidate_entity)
  if(nrow(candidate_entity_r) == 0)
  {
    stop("candidate not found")
  } else if (nrow(candidate_entity_r) > 1)
  {
    print(candidate_entity_r)
    stop("multiple candidates match regex")
  }
  candidate_committees = DB$project(DB$equi_join(LINKAGE, candidate_entity, "'left_names=candidate_idx'", "'right_names=entity_idx'", "'keep_dimensions=1'"), committee_idx) 
  candidate_committees_r = as.R(candidate_committees)
  candidate_entities = data.frame(entity_idx= c(candidate_committees_r$committee_idx, candidate_entity_r$entity_idx))
  candidate_entities = as.scidb(DB, candidate_entities, types=c("int64"))
  candidate_transactions = DB$equi_join(TRANSACTION, candidate_entities, "'left_names=to_entity_idx'", "'right_names=entity_idx'", "'keep_dimensions=1'")
  candidate_transactions_by_date = DB$grouped_aggregate(candidate_transactions, 
                                "count(*) as num_transactions", 
                                "sum(transaction_amount) as total",
                                transaction_date_int)
  candidate_transactions_by_date = as.R(candidate_transactions_by_date, only_attributes=T)
  dates = as.Date(as.character(candidate_transactions_by_date$transaction_date_int), format="%Y%m%d")
  series = xts(data.frame(
              total=candidate_transactions_by_date$total,
              num_transactions=candidate_transactions_by_date$num_transactions), order.by=dates)
  series = series['2014/']
  dygraph(series)
}
