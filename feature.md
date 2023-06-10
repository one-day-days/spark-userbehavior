现在我有两个数据集。数据集userBehavior是2014年11月18日到2014年12月18日的淘宝用户行为数据，字段有以及字段说明如下，
==user_id==: 用户标识id,
==item_id==:商品标识,
==behavior_type==:用户对商品的行为类型,包括点击、收藏、加购物车、购买，对应取值分别是1、2、3、4，
==item_category==:商品种类标识,
==date==:行为发生的日期如"2014-12-02",
==hour:==行为发生的时间精确到小时。
另一个商品数据集categoryItem,字段如下，==item_id==：商品标识，==item_category==：商品种类标识。这两个数据集存储格式为CSV。

现有有以下特征，这些特征均有2014-12-17到2014-12-18之间的数据计算得到：

**用户特征**

1.==userBehavior_latset==：用户最近点击时间、收藏时间、加购物车时间、购买时间。
字段有==user_id,u1_latest_click_time，u1_latest_collect_time，u1_latest_add_cart_time，u1_latest_buy_time==。

2.==userBehavior_count==：用户点击量、收藏量、加购物车量、购买量。
字段有==user_id,u2_click_count，u2_collect_count，u2_add_cart_count，u2_buy_count==。

3.==userBehavior_percetconv==：用户转化率即用户购买量分别除以用户点击、收藏、加购物车这三类行为数。
==user_id,u3_click_count_pc,u3_collect_count_pc,u3_add_cart_count_pc==

4.==userBehavior_std==：用户点击、收藏、加购物车、购买量在28天里的均值方差。
==user_id,avg_click,avg_collect,avg_add_cart,avg_buy,std_click,std_collect,std_add_cart,std_buy==

**商品特征**

5.==comdity_click_collect_addcart_buy_cnt==：商品被点击、收藏、加购物车、购买量。
==item_id,i1_click_cnt,i1_collect_cnt,i1_addcart_cnt,i1_buy_cnt==

6.==comdity_click_collect_addcart_buy_pc==：商品被购买转化率。
==item_id,i2_buy_click_pc，i2_buy_collect_pc，i2_buy_addcat_pc==

7.==omdity_click_collect_addcart_buy_std==：商品被点击、收藏、加购物车、购买量在28天里的均值与方差。
==item_id,avg_click_cnt,std_click_cnt,avg_collect_cnt,std_collect_cnt,avg_addcart_cnt,std_addcart_cnt,avg_buy_cnt,std_buy_cnt==

**用户与商品特征**

8.==userItemBehavior_latest==：用户对商品点击、收藏、加购物车、购买的最近时间

==user_id,item_id,ui1_latest_click_time,ui1_latest_favorite_time,ui1_latest_cart_time,ui1_latest_purchase_time==

9.==userItemBehavior_cnt==用户对商品点击、收藏、加购物车、购买的次数

==user_id,item_id,ui2_click_cnt,ui2_favorite_cnt,ui2_cart_cnt,ui2_purchase_cnt==

**商品种类特征**

10.categoryBehaviorCnt：该类商品被点击、收藏、加购物车、购买量。

==item_category,c1_click_cnt，c1_favorite_cnt，c1_cart_cnt，c1_purchase_cnt==

11.categoryBehaviorPC：该类商品转化率。
==item_category,c2_click_favorite_pc,c2_click_cart_pc,c2_click_purchase_pc==

**用户-商品-类别组合特征**

12.==userItemCategoryRatio==：用户对商品的点击、收藏、加购物车、购买次数除用户对该商品所属分类（item_category）的点击、收藏、加购物车、购买次数。
==user_id,item_id,click_ratio,favorite_ratio,cart_ratio,purchase_ratio==

**交叉特征**

13.用户对商品最近点击、收藏、加购物车、购买时间减去该用户最近点击、收藏、加购物车、购买时间f1。

==user_id,item_id,f1_diff_click_time,f1_diff_favorite_time,f1_diif_cart_time,f1_diff_purchase_time==

14.用户对商品最近点击、收藏、加购物车、购买时间减去该用户购买时间f2。

==user_id,item_id,f2_diff_click_time,f2_diff_favorite_time,f2_diif_cart_time,f2_diff_purchase_time==

15、用户对商品点击、收藏、加购物车、购买量减去用户平均点击、收藏、加购物车、购买量f3。

==user_id,item_id,f3_diff_click,f3_diff_favorite,f3_diif_cart,f3_diff_purchase==

16、用户对商品点击、收藏、加购物车、购买量减去商品平均被点击、收藏、加购物车、购买量f4。

==user_id,item_id,f4_diff_click,f4_diff_favorite,f4_diif_cart,f4_diff_purchase==

17、用户对商品点击、收藏、加购物车、购买量除以用户点击、收藏、加购物车、购买量f5。

==user_id,item_id,f5_ratio_click,f5_ratio_favorite,f5_ratio_cart,f5_ratio_purchase==

18.商品被点击、收藏、加购物车、购买量除以该类商品被点击、收藏、加购物车、购买量f8。

==item_id,f8_ratio_click,f8_ratio_favorite,f8_ratio_cart,f8_ratio_purchase==

这些特征均为orc格式储存，现需要运用这些特征预测2014-12-18用户会购买的商品，请用SparkMlib编写。