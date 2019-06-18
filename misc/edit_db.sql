use db_mkt_track;

TRUNCATE a_index_bias;
TRUNCATE a_index_bias_quantile;
TRUNCATE a_share_bias;
TRUNCATE a_share_bias_quantile;
TRUNCATE a_share_pb_quantile;
TRUNCATE a_share_pe_quantile;
TRUNCATE a_share_alpha;
TRUNCATE a_share_alpha_quantile;
TRUNCATE a_share_alpha_dd;
TRUNCATE a_share_alpha_dd_quantile;



DELETE FROM a_index_eod_prices WHERE trade_date > '20190610';
DELETE FROM a_share_eod_prices WHERE trade_date > '20190610';
DELETE FROM a_share_fin_pit WHERE trade_date > '20190610';

