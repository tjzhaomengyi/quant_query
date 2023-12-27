/**
  常用查询
 */
select distinct (accid) from quant.deal;
select * from quant.deal where accid = "410001025561" ;


#查询股票信息
select tradetime, stockid, stockname, ordersysid, offsetflagtype from quant.deal where stockid in ('SH.600665',
'SH.600971',
'SH.601068',
'SH.603998',
'SZ.000923',
'SZ.002008',
'SZ.002170',
'SZ.002443',
'SZ.002972',
'SZ.300050',
'SZ.300161',
'SZ.300183',
'SZ.300315',
'SZ.300500',
'SZ.300816') and accid='410030031937' group by ordersysid;

#根据用户ID对买入账单进行聚合
select tradetime as buytime, stockid, stockname, ordersysid, sum(volume) as buy_volume, price as buy_price,  price * sum(volume) as buy_mount,
       row_number() over (partition by stockid order by tradetime) as buy_trade_rank from quant.deal
    where accid='410030031937' and offsetflagtype='48'  and stockname="国投资本" group by ordersysid;

#根据用户ID对卖出账单进行聚合
select tradetime as selltime, stockid, stockname, ordersysid, sum(volume) as sell_volume, price as sell_price,  price * sum(volume) as sell_mount,
       row_number() over (partition by stockid order by tradetime) as selll_trade_rank from quant.deal
    where accid='410030031937' and offsetflagtype='49' and stockid='SZ.002502' group by ordersysid;

select tradetime, stockid, stockname, ordersysid, sum(volume) , price ,  price * sum(volume),
       row_number() over (partition by stockid order by tradetime) as buy_trade_rank, offsetflagtype from quant.deal
    where accid='410030031937'  and stockid="SZ.002502" and deal.offsetflagtype = '48' group by ordersysid;

#买入数量统计
         select stockid, count(*) as buy_cnt from
                  (select tradetime as buytime,stockid,stockname,ordersysid, sum(volume) as buy_volume,price as buy_price,
                          price * sum(volume) as buy_mount,row_number() over (partition by stockid order by tradetime) as buy_trade_rank, ordersysid as buy_order
                   from quant.deal
                   where accid = '410030031937' and offsetflagtype = '48' group by ordersysid) buy_tmp group by stockid;
#卖出数量统计
select stockid, count(*) as sell_cnt from
(select tradetime as selltime, stockid, stockname, ordersysid, sum(volume) as sell_volume,
                       price as sell_price, price * sum(volume) as sell_mount,
                       row_number() over (partition by stockid order by tradetime) as sell_trade_rank, ordersysid as sell_order from quant.deal
                   where accid='410030031937' and offsetflagtype='49' group by ordersysid) sell_tmp group by stockid;

#清洗卖出买入数量的问题，卖出大于买入SH.600061


select buy_statics.stockid, buy_cnt, sell_cnt from(
(select stockid, count(*) as buy_cnt from
                  (select tradetime as buytime,stockid,stockname,ordersysid, sum(volume) as buy_volume,price as buy_price,
                          price * sum(volume) as buy_mount,row_number() over (partition by stockid order by tradetime ) as buy_trade_rank, ordersysid as buy_order
                   from quant.deal
                   where accid = '410030031937' and offsetflagtype = '48' group by ordersysid) buy_tmp group by stockid) buy_statics
join
(select stockid, count(*) as sell_cnt from
(select tradetime as selltime, stockid, stockname, ordersysid, sum(volume) as sell_volume,
                       price as sell_price, price * sum(volume) as sell_mount,
                       row_number() over (partition by stockid order by tradetime) as sell_trade_rank, ordersysid as sell_order from quant.deal
                   where accid='410030031937' and offsetflagtype='49' group by ordersysid) sell_tmp group by stockid) sell_statics on buy_statics.stockid = sell_statics.stockid)
where sell_cnt > buy_cnt;



#聚合买卖信息,单账号
select buy_tmp.stockid, buy_tmp.stockname, buytime, selltime, DATEDIFF(selltime, buytime) as days_diff,sell_mount-buy_mount as profit, buy_price, sell_price,
    buy_volume, sell_volume,  buy_order, sell_order from (
                  (select tradetime as buytime,stockid,stockname,ordersysid, sum(volume) as buy_volume,price as buy_price,
                          price * sum(volume) as buy_mount,row_number() over (partition by stockid order by tradetime) as buy_trade_rank, ordersysid as buy_order
                   from quant.deal
                   where  accid = '410030031937' and offsetflagtype = '48' group by ordersysid) buy_tmp
                    left join
                  (select tradetime as selltime, stockid, stockname, ordersysid, sum(volume) as sell_volume,
                       price as sell_price, price * sum(volume) as sell_mount,
                       row_number() over (partition by stockid order by tradetime) as sell_trade_rank, ordersysid as sell_order from quant.deal
                   where  accid = '410030031937' and offsetflagtype='49' group by ordersysid) sell_tmp
on buy_tmp.stockid=sell_tmp.stockid and buy_tmp.buy_trade_rank=sell_tmp.sell_trade_rank and selltime > buytime) where selltime is not null;

select * from quant.deal where stockid='SZ.002502';
#集合信息,所有账户
select buy_tmp.accid as accid, buy_tmp.stockid, buy_tmp.stockname, buytime, selltime, DATEDIFF(selltime, buytime) as days_diff,sell_mount-buy_mount as profit, buy_price, sell_price,
    buy_volume, sell_volume,  buy_order, sell_order from (
                  (select accid, tradetime as buytime,stockid,stockname,ordersysid, sum(volume) as buy_volume,price as buy_price,
                          price * sum(volume) as buy_mount,row_number() over (partition by accid,stockid order by tradetime) as buy_trade_rank, ordersysid as buy_order
                   from quant.deal
                   where offsetflagtype = '48' group by accid, ordersysid) buy_tmp
                    left join
                  (select accid, tradetime as selltime, stockid, stockname, ordersysid, sum(volume) as sell_volume,
                       price as sell_price, price * sum(volume) as sell_mount,
                       row_number() over (partition by accid,stockid order by tradetime) as sell_trade_rank, ordersysid as sell_order from quant.deal
                   where  offsetflagtype='49' group by accid, ordersysid) sell_tmp
on buy_tmp.accid = sell_tmp.accid and buy_tmp.stockid=sell_tmp.stockid and buy_tmp.buy_trade_rank=sell_tmp.sell_trade_rank and selltime > buytime) where selltime is not null;