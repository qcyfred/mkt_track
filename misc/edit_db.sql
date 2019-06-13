use db_mkt_track;

truncate a_index_bias;
truncate a_index_bias_quantile;
truncate a_share_bias;
truncate a_share_bias_quantile;
truncate a_share_pb_quantile;
truncate a_share_pe_quantile;


DELETE FROM a_index_eod_prices WHERE trade_date > '20190610';
DELETE FROM a_share_eod_prices WHERE trade_date > '20190610';
DELETE FROM a_share_fin_pit WHERE trade_date > '20190610';

