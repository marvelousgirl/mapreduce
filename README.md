# mapreduce
用mapreduce计算框架实现了3个小demo: wordcount、基于物品的推荐算法(itemCF)和基于用户的推荐算法(userCF)
itemCF步骤：
step1: 根据用户行为列表构建评分矩阵
map输入：key:LongWritable类型，每一行的起始偏移量    value: Text类型 userID,itemID,score
map输出：key:Text类型  itemID     value: Text类型 userID_score
reduce输入：key:Text类型  itemID     value: Text类型 <userID1_score, userID2_score, userID2_score, ...>
reduce输出：key:Text类型  itemID     value: Text类型 userID1_score,userID2_score,userID3_score

step2: 利用step1得到的评分矩阵，构建物品与物品的相似度矩阵，此处的相似度度量方法采用余弦相似度
此外，评分矩阵还要作为缓存，在setup方法里实现
map输入：key:LongWritable类型，每一行的起始偏移量    value: Text类型 itemID  userID1_score,userID2_score,userID3_score
map输出：key:Text类型，itemID  value:Text类型 itemID1_sim
reduce输入：key:Text类型，itemID  value:Text类型 <itemID1_sim,itemID3_sim,...>
reduce输出：key:Text类型 itemID  value: Text类型 itemID1_sim,itemID3_sim,itemID5_sim

step3: 将评分矩阵转置
map输入：key:LongWritable类型，每一行的起始偏移量    value: Text类型 itemID  userID1_score,userID2_score,userID3_score
map输出：key:Text类型 userID  value: Text类型 itemID_score
reduce输入：key: Text类型 userID  value: Text类型 <itemID1_score,itemID3_score,...>
reduce输出：key: Text类型 userID  value: Text类型 itemID1_score,itemID3_score,itemID2_score

step4: 物品与物品的相似度矩阵 * 转置后的评分矩阵
此时，转置后的评分矩阵要作为缓存，在setup方法里实现
map输入：key:LongWritable类型，每一行的起始偏移量   Text类型 itemID  itemID1_sim,itemID3_sim,itemID5_sim
map输出：key:Text类型 itemID  value: Text类型 userID_score
reduce输入：key:Text类型 itemID  value: Text类型 <userID1_score, userID2_score,...>
reduce输出：key:Text类型 itemID  value: Text类型 userID1_score, userID2_score,userID3_score

step5: 根据评分矩阵，将用户已有过行为的商品忽略
此时，评分矩阵作为缓存，在setup方法里实现
map输入：key:LongWritable类型，每一行的起始偏移量  value: Text类型 itemID userID1_score, userID2_score,userID3_score
map输出：key:Text类型 userID  value: Text类型  itemID_score
reduce输入：key:Text类型 userID  value: Text类型  <itemID1_score, itemID3_score,...>
reduce输出：key:Text类型 userID  value: Text类型  itemID1_score,itemID3_score,itemID5_score


userCF:
和itemCF的逻辑是一样的，区别在于以userID作为行
