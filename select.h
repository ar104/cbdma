#ifndef _SELECT_
#define _SELECT_

typedef struct {
  unsigned long	ss_sold_date_sk;
  unsigned long	ss_sold_time_sk;
  unsigned long	ss_sold_item_sk;
  unsigned long	ss_sold_customer_sk;
  unsigned long	ss_sold_cdemo_sk;
  unsigned long	ss_sold_hdemo_sk;
  unsigned long	ss_sold_addr_sk;
  unsigned long	ss_sold_store_sk;
  unsigned long	ss_sold_promo_sk;
  unsigned long	ss_ticket_number;
  int quantity;
  float wholesale_cost;
  float list_price;
  float sales_price;
  float coupon_amt;
  float ext_sales_price;
  float ext_wholesale_cost;
  float ext_list_price;
  float ext_tax;
  float coupon_amt2;
  float net_paid;
  float net_paid_inc_tax;
  float net_profit;
} sales_table_row_t;

int select_year(sales_table_row_t *r,
		unsigned long left_closed,
		unsigned long right_open)
{
  if(r->ss_sold_date_sk >= left_closed && r->ss_sold_date_sk < right_open)
    return 1;
  else
    return 0;
}

int select_dow(sales_table_row_t *r,
	       unsigned long pivot)
{
  if((r->ss_sold_date_sk - pivot)%7 == 0)
    return 1;
  else
    return 0;
}

int select_store(sales_table_row_t *r,
		 unsigned long store)
{
  if(r->ss_sold_store_sk == store)
    return 1;
  else
    return 0;
}

#endif
