# 实现思路， b -tree && clusterd index
设计的重要class； 
1. bufferReader, 里面有LRUcache,用来存放一定数目的btree节点，同时有着读入源文件的内容的功能，他每次一次性读入一定数目源文件中的内容作为cache放在文件中
2. BTreeNode, 一个InNodePos的类，用来表示key在node中的位置和其对应在源文件中value的位置，不将键值对同时存入的原因是担心如果value太大的话放不在内存中，同时每个node不用存储value可以更加紧凑，减少miss的情况，bTree节点还存放这key的数据，用byte array的方式存，如果当对应的key很大的时候将其存放在文件中，这样的好处，当key都较小的时候存放在memory中，key较大就将他放在文件中，必要的时候读取，兼具灵活性和鲁棒性，每个node都对应一个 tree file和 data file, tree file用来存储btree的结构， data file 存放key 值。这样方便flush到disk上和从disk快速读取。
3. KVstore, 可以实现loadFile，如果该 file有对应的 btree 相关file的话，可以利用那个直接检索，否则就对原始file一边检索一边生成， read(), 检索， close，将内存中的btree相关数据flush到磁盘上
缺点：
1. 没有实现并发访问
 2. 对于接近内存大小的key没有处理有可能会造成问题
 3. 局部性做的并不好，譬如可以将整个btree存为一个f文件, 性能会更好，node之内的l局部性也不够好，主要考虑到有可能存在特别大的key.