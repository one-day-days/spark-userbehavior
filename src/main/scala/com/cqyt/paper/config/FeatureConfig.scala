package com.cqyt.paper.config

object FeatureConfig {

  val userBehavior_latset = "file:///F:/feature/user/userBehavior_latest"
  val userBehavior_count = "file:///F:/feature/user/userBehavior_count"
  val userBehavior_percetconv = "file:///F:/feature/user/userBehavior_percetconv"
  val userBehavior_std = "file:///F:/feature/user/userBehavior_std"
  val comdity_click_collect_addcart_buy_cnt = "file:///F:/feature/commodity/comdity_click_collect_addcart_buy_cnt"
  val comdity_click_collect_addcart_buy_pc = "file:///F:/feature/commodity/comdity_click_collect_addcart_buy_pc"
  val comdity_click_collect_addcart_buy_std = "file:///F:/feature/commodity/comdity_click_collect_addcart_buy_std"
  val userItemBehavior_latest = "file:///F:/feature/userItem/userItemBehavior_latest"
  val userItemBehavior_cnt = "file:///F:/feature/userItem/userItemBehavior_cnt"
  lazy val userItemCategoryRatio = "file:///F:/feature/userItemCategoryRatio"
  val categoryBehaviorCnt = "file:///F:\\feature\\category\\categoryBehaviorCnt"
  val categoryBehaviorPC = "file:///F:\\feature\\category\\categoryBehaviorPC"
  val f8 = "file:///F:/feature/intersect/f8"
  val f1 = "file:///F:/feature/intersect/f1"
  val f2 = "file:///F:/feature/intersect/f2"
  val f3 = "file:///F:/feature/intersect/f3"
  val f4 = "file:///F:/feature/intersect/f4"
  val f5 = "file:///F:/feature/intersect/f5"

  lazy val  featureColmuns = Array("u1_latest_click_time", "u1_latest_collect_time", "u1_latest_add_cart_time", "u1_latest_buy_time",
    "u2_click_count", "u2_collect_count", "u2_add_cart_count", "u2_buy_count",
    "u3_click_count_pc", "u3_collect_count_pc", "u3_add_cart_count_pc",
    "avg_click", "avg_collect", "avg_add_cart", "avg_buy", "std_click", "std_collect", "std_add_cart", "std_buy",
    "i1_click_cnt", "i1_collect_cnt", "i1_addcart_cnt", "i1_buy_cnt",
    "i2_buy_click_pc", "i2_buy_collect_pc", "i2_buy_addcat_pc",
    "avg_click_cnt", "std_click_cnt", "avg_collect_cnt", "std_collect_cnt","avg_addcart_cnt", "std_addcart_cnt", "avg_buy_cnt", "std_buy_cnt",
    "ui1_latest_click_time", "ui1_latest_favorite_time", "ui1_latest_cart_time", "ui1_latest_purchase_time",
    "ui2_click_cnt", "ui2_favorite_cnt", "ui2_cart_cnt", "ui2_purchase_cnt","c1_click_cnt", "c1_favorite_cnt", "c1_cart_cnt", "c1_purchase_cnt",
    "c2_click_favorite_pc", "c2_click_cart_pc", "c2_click_purchase_pc",
    "f1_diff_click_time", "f1_diff_favorite_time", "f1_diif_cart_time", "f1_diff_purchase_time",
    "f2_diff_click_time", "f2_diff_favorite_time", "f2_diif_cart_time", "f2_diff_purchase_time",
    "f3_diff_click", "f3_diff_favorite", "f3_diif_cart", "f3_diff_purchase",
    "f4_diff_click", "f4_diff_favorite", "f4_diif_cart", "f4_diff_purchase",
    "f5_ratio_click", "f5_ratio_favorite", "f5_ratio_cart", "f5_ratio_purchase",
    "f8_ratio_click", "f8_ratio_favorite", "f8_ratio_cart", "f8_ratio_purchase")

}
