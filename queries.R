library(scidb)
DB = scidbconnect()

ENTITY = scidb(DB, "ENTITY")
TRANSACTION = scidb(DB, "TRANSACTION")
CCL = scidb(DB, "CCL")

library('xts')
library('dygraphs')

TYPE_CANDIDATE  = 1
TYPE_PAC        = 2
TYPE_INDIVIDUAL = 3

#lookup_entity('NATIONAL.*RIFLE', 2)
#lookup_entity(entity_id='P00003392')
lookup_entity = function(entity_regex, entity_type, entity_id)
{
  if(!missing(entity_id))
  {
    if(!missing(entity_regex) || !missing(entity_type))
    {
      stop("Please provide either entity ID or regex and optional type")
    }
    filter_expr=paste0("entity_id = '",entity_id,"'")
  } else
  {
    if(missing(entity_regex))
    {
      stop("Please provide either entity ID or regex and optional type")
    }
    filter_expr = sprintf("regex(entity_name, '.*%s.*')", entity_regex)
    if(!missing(entity_type))
    {
      filter_expr = paste(filter_expr, "and entity_type=", entity_type)
    }
  }
  as.R(DB$filter(ENTITY, R(filter_expr)))
}

get_transactions_to_entity = function(entity_regex, entity_type, entity_id)
{
  entity = lookup_entity(entity_regex, entity_type, entity_id)
  if(nrow(entity)==0)
  {
    stop("No entity found")
  } else if(nrow(entity)>1)
  {
    print(entity)
    stop("multiple entities match, be more specific")
  }
  associated_committees = DB$filter(LINKAGE, R(paste0("candidate_idx=",entity$entity_idx)))
  associated_committees_r = as.R(associated_committees)
  all_entities = data.frame(entity_idx= c(associated_committees_r$committee_idx, entity$entity_idx))
  
  all_entities = as.scidb(DB, all_entities, types=c("int64"))
  transactions = DB$equi_join(TRANSACTION, all_entities, "'left_names=to_entity_idx'", "'right_names=entity_idx'", "'keep_dimensions=1'")
  transactions_by_date = DB$grouped_aggregate(transactions, 
                                "count(*) as num_transactions", 
                                "sum(transaction_amount) as total",
                                transaction_date_int)
  transactions_by_date = as.R(transactions_by_date, only_attributes=T)
  dates = as.Date(as.character(transactions_by_date$transaction_date_int), format="%Y%m%d")
  series = xts(data.frame(
              total=transactions_by_date$total,
              num_transactions=transactions_by_date$num_transactions), order.by=dates)
  return(series)
}

timeseries_example = function()
{
  trump_transactions   = get_transactions_to_entity(entity_id='P80001571')
  jeb_transactions     = get_transactions_to_entity(entity_id='P60008059')
  cruz_transactions    = get_transactions_to_entity(entity_id='P60006111')
  rubio_transactions   = get_transactions_to_entity(entity_id='P60006723')
  kasich_transactions  = get_transactions_to_entity(entity_id='P60003670')
  
  rep = merge(trump_transactions, jeb_transactions, cruz_transactions, rubio_transactions, kasich_transactions)
  rep = rep[, c(1,3,5,7,9)]
  colnames(rep) = c("trump", "jeb", "cruz", "rubio", "kasich")
  rep = rep["2015/"]
  dygraph(rep) %>%
    dyRangeSelector() %>% 
    dyEvent('2016-07-18', 'REPUBLICAN NATIONAL CONVENTION', labelLoc = "top") %>%
    dyEvent('2015-08-06', 'FIRST PRIMARY DEBATE', labelLoc = "top") %>%
    dyEvent('2016-03-10', 'LAST PRIMARY DEBATE', labelLoc = "top") %>%
    dyEvent('2016-05-04', 'ALL EXCEPT TRUMP SUSPENDED', labelLoc = "top") %>%
    dyEvent('2016-11-08', 'GENERAL ELECTION', labelLoc = "top") %>%
    dyLegend(show = "follow") %>%
    dyOptions(stackedGraph=TRUE,labelsKMB = "M") %>%
    dyRoller(rollPeriod = 1)
  
  bernie_transactions  = get_transactions_to_entity(entity_id='P60007168')
  hillary_transactions = get_transactions_to_entity(entity_id='P00003392')
  dem = merge(bernie_transactions, hillary_transactions)
  dem = dem[, c(1,3)]
  colnames(dem) = c("bernie", "hillary")
  dem = dem["2015/"]
  dem_v = merge(bernie_transactions, hillary_transactions)
  dem_v = dem_v[, c(2,4)]
  colnames(dem_v) = c("bernie", "hillary")
  dem_v = dem_v["2015/"]
  browsable(tagList(
    dygraph(dem, group="1", main="Transaction Dollar Amount") %>%
     dyOptions(stackedGraph=TRUE,labelsKMB = "M") %>% 
     dyRangeSelector() %>% 
     dyLegend(show = "follow") %>%
     dyEvent('2016-11-08', 'GENERAL ELECTION', labelLoc = "top") %>%
     dyRoller(rollPeriod = 1),
    dygraph(dem_v, group="1", main="Transaction Volume") %>%
     dyOptions(stackedGraph=TRUE,labelsKMB = "M") %>% 
     dyRangeSelector() %>% 
     dyLegend(show = "follow") %>%
     dyEvent('2016-11-08', 'GENERAL ELECTION', labelLoc = "top") %>%
     dyRoller(rollPeriod = 1)))
  
  hvt = merge(hillary_transactions, trump_transactions)
  hvt = hvt[, c(1,3)]
  colnames(hvt) = c("hillary", "trump")
  hvt = hvt["2015/"]
  hvt_v = merge(hillary_transactions, trump_transactions)
  hvt_v = hvt_v[, c(2,4)]
  colnames(hvt_v) = c("hillary", "trump")
  hvt_v = hvt_v["2015/"]
  browsable(tagList(
    dygraph(hvt, group="1", main="Transaction Dollar Amount", height=400) %>%
      dyOptions(stackedGraph=TRUE,labelsKMB = "M") %>% 
      dyLegend(show = "follow") %>%
      dyEvent('2016-11-08', 'GENERAL ELECTION', labelLoc = "top") %>%
      dyEvent('2016-09-26', 'FIRST DEBATE', labelLoc = "top") %>%
      dyEvent('2016-10-04', 'SECOND DEBATE', labelLoc = "top") %>%
      dyEvent('2016-10-09', 'VP DEBATE', labelLoc = "top") %>%
      dyEvent('2016-10-19', 'THIRD DEBATE', labelLoc = "top") %>%
      dyRoller(rollPeriod = 1),
   dygraph(hvt_v, group="1", main="Transaction Volume", height=400) %>%
     dyOptions(stackedGraph=TRUE,labelsKMB = "M") %>% 
     dyRangeSelector() %>% 
     dyLegend(show = "follow") %>%
     dyEvent('2016-11-08', 'GENERAL ELECTION', labelLoc = "top") %>%
     dyEvent('2016-09-26', 'FIRST DEBATE', labelLoc = "top") %>%
     dyEvent('2016-10-04', 'SECOND DEBATE', labelLoc = "top") %>%
     dyEvent('2016-10-09', 'VP DEBATE', labelLoc = "top") %>%
     dyEvent('2016-10-19', 'THIRD DEBATE', labelLoc = "top") %>%
     dyRoller(rollPeriod = 1)
  ))
}

top_contributors_to_entity = function(entity_idx=4327273, n=10, start_date="null", end_date="null")
{
  transactions = DB$between(TRANSACTION, null, R(entity_idx), R(start_date), null, null, R(entity_idx), R(end_date), null)
  transactions_by_donor = DB$grouped_aggregate(transactions, 
                                "sum(transaction_amount) as total",
                                "from_entity_idx")
  transactions_by_donor = DB$sort(transactions_by_donor, "total desc")
  transactions_by_donor = store(DB, transactions_by_donor, temp=T)
  top_n = as.R(DB$between(transactions_by_donor, 0, R(n)))
  other = as.R(DB$aggregate(DB$between(transactions_by_donor, R(n+1), null), "sum(total) as total"))
  if(other$total > 0)
  {
    result = data.frame( from_entity_idx = c(top_n$from_entity_idx, -1),
                         total = c(top_n$total, other$total),
                         to_entity_idx = rep(entity_idx, nrow(top_n)+1))
  } else
  {
    result = data.frame( from_entity_idx = top_n$from_entity_idx,
                         total = top_n$total,
                         to_entity_idx = rep(entity_idx, nrow(top_n)))
  }
  return(result)
}

get_entity_metadata = function(entity_idx_vector)
{
  entities = as.scidb(DB, entity_idx_vector, types=c("int64"))
  entities = as.R(DB$equi_join(entities, ENTITY, "'left_names=val'", "'right_names=entity_idx'", "'left_outer=T'"))
  entity_names = paste(entities$entity_id, "|", entities$entity_name)
  entity_names[is.na(entity_names)] = "OTHER"
  entity_types = entities$entity_type
  entity_types[is.na(entity_types)] = "OTHER"
  return(data.frame(entity_name=entity_names, entity_type=entity_types))
}

candidate_contributor_network = function(candidate_idx=2570157, fanout=20, depth=2, start_date="null", end_date="null")
{
  candidate_pacs = as.R(DB$between(LINKAGE, R(candidate_idx), null, R(candidate_idx), null))
  result = data.frame(from_entity=c(), total=c(), to_entity=c())
  to_lookup = candidate_idx
  visited = c()
  if(nrow(candidate_pacs)>0)
  {
    result=data.frame(from_entity_idx=candidate_pacs$committee_idx, total=rep(-1, nrow(candidate_pacs)), to_entity_idx=candidate_pacs$candidate_idx)
    to_lookup = c(to_lookup, candidate_pacs$committee_idx)
  }
  while(depth>0)
  {
    next_lookup = c()
    for(i in 1:length(to_lookup))
    {
      idx=to_lookup[i]
      contrib = top_contributors_to_entity(idx, fanout, start_date, end_date)
      visited = c(visited, idx)
      if(nrow(contrib)==0)
        next
      result = rbind(result, contrib)
      print(result)
      new_ids = contrib$from_entity_idx
      new_ids = new_ids[new_ids!=-1]
      new_ids = new_ids [ !(new_ids %in% visited) ]
      next_lookup = c(next_lookup, new_ids)
    }
    to_lookup = unique(next_lookup)
    depth = depth-1
  }
  return(result)
}

foo = function()
{
  result = candidate_contributor_network(depth=2) 
  result = subset(result, from_entity_idx!=-1) ###MMEBE
  indeces = unique(c(result$from_entity_idx, result$to_entity_idx))
  links = data.frame(Source=match(result$from_entity_idx, indeces)-1,
                     Target=match(result$to_entity_idx, indeces)-1,
                     Value= result$total)
  nodes = get_entity_metadata(indeces)
  forceNetwork(links, nodes, "Source", "Target", "Value", "entity_name", Group="entity_type",
               linkWidth = networkD3::JS("function(d) { return Math.sqrt(d.value/1000000); }"))
  
  
  result = candidate_contributor_network(depth=3, fanout=20, start_date="20150101", end_date="20160301") 
  result = subset(result, from_entity_idx!=-1) ###MMEBE
  indeces = unique(c(result$from_entity_idx, result$to_entity_idx))
  links = data.frame(Source=match(result$from_entity_idx, indeces)-1,
                     Target=match(result$to_entity_idx, indeces)-1,
                     Value= result$total)
  nodes = get_entity_metadata(indeces)
  forceNetwork(links, nodes, "Source", "Target", "Value", "entity_name", Group="entity_type",
               linkWidth = networkD3::JS("function(d) { return Math.sqrt(d.value/1000000); }"),
               opacity=1)
  
  #PAUL RYAN
  result = candidate_contributor_network(candidate_idx=2184276, depth=3, fanout=10, start_date="null", end_date="null") 
  result = subset(result, from_entity_idx!=-1) ###MMEBE
  indeces = unique(c(result$from_entity_idx, result$to_entity_idx))
  links = data.frame(Source=match(result$from_entity_idx, indeces)-1,
                     Target=match(result$to_entity_idx, indeces)-1,
                     Value= result$total)
  nodes = get_entity_metadata(indeces)
  forceNetwork(links, nodes, "Source", "Target", "Value", "entity_name", Group="entity_type",
               linkWidth = networkD3::JS("function(d) { return Math.sqrt(d.value/1000000); }"),
               opacity=1)
  
  
  
}



  