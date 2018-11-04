# mapreduce
用mapreduce计算框架实现了3个小demo: wordcount、基于物品的推荐算法(itemCF)和基于用户的推荐算法(userCF) <br>
## itemCF步骤： <br>
<img src="https://github.com/marvelousgirl/mapreduce/blob/master/images/itemCF.png" width="150" height="150" alt="图片加载失败时，显示这段字"/>  
### step1: 根据用户行为列表构建评分矩阵 <br>
map输入：key:LongWritable类型，每一行的起始偏移量    value: Text类型 userID,itemID,score <br>
map输出：key:Text类型  itemID     value: Text类型 userID_score <br>
reduce输入：key:Text类型  itemID     value: Text类型 <userID1_score, userID2_score, userID2_score, ...> <br>
reduce输出：key:Text类型  itemID     value: Text类型 userID1_score,userID2_score,userID3_score <br>

### step2: 利用step1得到的评分矩阵，构建物品与物品的相似度矩阵，此处的相似度度量方法采用余弦相似度 <br>
此外，评分矩阵还要作为缓存，在setup方法里实现 <br>
map输入：key:LongWritable类型，每一行的起始偏移量    value: Text类型 itemID  userID1_score,userID2_score,userID3_score <br>
map输出：key:Text类型，itemID  value:Text类型 itemID1_sim <br>
reduce输入：key:Text类型，itemID  value:Text类型 <itemID1_sim,itemID3_sim,...> <br>
reduce输出：key:Text类型 itemID  value: Text类型 itemID1_sim,itemID3_sim,itemID5_sim <br>

### step3: 将评分矩阵转置 <br>
map输入：key:LongWritable类型，每一行的起始偏移量    value: Text类型 itemID  userID1_score,userID2_score,userID3_score <br>
map输出：key:Text类型 userID  value: Text类型 itemID_score <br>
reduce输入：key: Text类型 userID  value: Text类型 <itemID1_score,itemID3_score,...> <br>
reduce输出：key: Text类型 userID  value: Text类型 itemID1_score,itemID3_score,itemID2_score <br>

### step4: 物品与物品的相似度矩阵 * 转置后的评分矩阵 <br>
此时，转置后的评分矩阵要作为缓存，在setup方法里实现 <br>
map输入：key:LongWritable类型，每一行的起始偏移量   Text类型 itemID  itemID1_sim,itemID3_sim,itemID5_sim <br>
map输出：key:Text类型 itemID  value: Text类型 userID_score <br>
reduce输入：key:Text类型 itemID  value: Text类型 <userID1_score, userID2_score,...> <br>
reduce输出：key:Text类型 itemID  value: Text类型 userID1_score, userID2_score,userID3_score <br>

### step5: 根据评分矩阵，将用户已有过行为的商品忽略 <br>
此时，评分矩阵作为缓存，在setup方法里实现 <br>
map输入：key:LongWritable类型，每一行的起始偏移量  value: Text类型 itemID userID1_score, userID2_score,userID3_score <br>
map输出：key:Text类型 userID  value: Text类型  itemID_score <br>
reduce输入：key:Text类型 userID  value: Text类型  <itemID1_score, itemID3_score,...> <br>
reduce输出：key:Text类型 userID  value: Text类型  itemID1_score,itemID3_score,itemID5_score <br>


## userCF: <br>
和itemCF的逻辑是一样的，区别在于以userID作为行 <br>
<img src="https://github.com/marvelousgirl/mapreduce/blob/master/images/userCF.png" width="150" height="150" alt="图片加载失败时，显示这段字"/> 
