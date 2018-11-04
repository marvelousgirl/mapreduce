step1:
map1: 将userID和itemID互换位置
A,1,1 -> key: 1  value: A_1

reduce1: 将分数汇总
1 <A_1 A_1 B_1...> -> 1 A_2,B_1

step2:
计算物物相似度: 余弦相似度

step3:
将评分矩阵转置,即行变成userID

step4:
物品相似度矩阵 乘    转置后的评分矩阵

step5:
将用户已有过的商品评分置0