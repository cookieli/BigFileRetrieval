package lzx.retrieval;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;

import lzx.retrieval.storage.BTreeNode;

public class LRUcache {
	int capacity;
	LinkedHashMap<Integer, BTreeNode> cache;

	public LRUcache(int capacity) {
		this.capacity = capacity;
		cache = new LinkedHashMap<>();
	}
	
	public boolean isEmpty() {
		return size() == 0;
	}
	public boolean containsKey(Integer key) {
		if (cache.containsKey(key))
			return true;
		else
			return false;
	}

	public void put(Integer nodeId, BTreeNode node) {
		if (containsKey(nodeId)) {
			cache.remove(nodeId);
		}
		if(capacity == 0) {
			node.flushDataToFile();
			node.flushTreeToFile();
		}
		if (size() == capacity) {
			evict();
		}
		cache.put(nodeId,node);
	}

	public BTreeNode get(Integer nodeId) {
		BTreeNode node;
		if (containsKey(nodeId)) {
			node = cache.get(nodeId);
			put(nodeId, node);
			return node;
		}
		return null;
	}

	public BTreeNode evict() {
		Integer nodeId;
		Iterator<Integer> iterator = cache.keySet().iterator();
		while (iterator.hasNext()) {
			nodeId = iterator.next();
			BTreeNode n = cache.get(nodeId);
			if (n != null) {
				//System.out.println("in evict");
				n.flushDataToFile();
				n.flushTreeToFile();
				cache.remove(nodeId);
			}
			return n;

		}
		return null;
	}

	public int size() {
		return cache.size();
	}
	public Set<Integer> keySet() {
		return cache.keySet();
	}
	
	public void clear() {
		while(!cache.isEmpty()) {
			evict();
		}
	}
}
