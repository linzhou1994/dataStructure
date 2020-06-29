package com.datastructure.util.collection.map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class CollectionKeyTreeMap<E, T> {
    private static final Logger logger = LoggerFactory.getLogger(CollectionKeyTreeMap.class);

    private CollectionKeyTreeMapNode rootNode;

    private Map<E, CollectionKeyTreeMapNodeCache<E>> keyCode2IndexNodeListMap;

    public CollectionKeyTreeMap() {
        rootNode = new CollectionKeyTreeMapNode(this);
        keyCode2IndexNodeListMap = new HashMap<>();
    }

    /**
     * 查找子集
     *
     * @param fatherCollection
     * @return
     */
    public List<T> getAllChildCollection(Collection<E> fatherCollection) {
        if (CollectionUtils.isEmpty(fatherCollection)) {
            return new ArrayList<>();
        }
        List<E> sortKeyList = getSortKeyList(fatherCollection);
        List<T> rlt = rootNode.getAllChildCollection(sortKeyList);
        return rlt;
    }

    /**
     * 查找父集
     *
     * @param childCollection
     * @return
     */
    public T getOneFatherCollection(Set<E> childCollection) {
        if (CollectionUtils.isEmpty(childCollection)) {
            return null;
        }
        //对key集合排序
        List<E> sortKeyList = getSortKeyList(childCollection);
        //找到排序后的最后一个key
        E lastkeyCode = sortKeyList.get(sortKeyList.size() - 1);
        //找到最后一个key对应的indexNodeCache
        CollectionKeyTreeMapNodeCache indexNodeCache = keyCode2IndexNodeListMap.get(lastkeyCode);
        if (indexNodeCache == null) {
            return null;
        }
        //找到相似且结果集数量最少的集合
        List<CollectionKeyTreeMapNode> indexNodeList = indexNodeCache.getSimilarIndexNode(sortKeyList);
        if (CollectionUtils.isEmpty(indexNodeList)) {
            return null;
        }

        T rlt = null;
        for (CollectionKeyTreeMapNode<E, T> indexNode : indexNodeList) {
            rlt = indexNode.getOneFatherCollection(sortKeyList);
            if (rlt != null) {
                break;
            }
        }
        return rlt;
    }

    /**
     * 添加数据
     *
     * @param value
     * @return
     */
    public void put(Collection<E> key, T value) {
        if (value == null) {
            return;
        }
        List<E> sortKeyList = getSortKeyList(key);
        List<CollectionKeyTreeMapNode> newIndexNodeList = rootNode.addkeyCollectionNode(sortKeyList, 0, value);
        registeIndexNodeList(newIndexNodeList);

    }

    /**
     * 移除数据
     *
     * @param key
     * @return
     */
    public void remove(Collection<E> key) {
        List<E> sortKeyList = getSortKeyList(key);
        List<CollectionKeyTreeMapNode> rlt = rootNode.removekeyCollectionNode(sortKeyList);
        logOutIndexNodeList(rlt);
    }

    /**
     * 获取所有value
     *
     * @return
     */
    public List<T> getValues() {
        return rootNode.getAllData();
    }


    /**
     * 删除的节点，要注销掉在树中的缓存
     *
     * @param indexNodeList 删除的节点的集合
     */
    private void logOutIndexNodeList(List<CollectionKeyTreeMapNode> indexNodeList) {
        if (CollectionUtils.isNotEmpty(indexNodeList)) {
            for (CollectionKeyTreeMapNode indexNode : indexNodeList) {
                logOutIndexNode(indexNode);
            }
        }
    }

    private void logOutIndexNode(CollectionKeyTreeMapNode indexNode) {
        if (indexNode != null) {
            CollectionKeyTreeMapNodeCache indexNodeCache = keyCode2IndexNodeListMap.get(indexNode.getKey());
            indexNodeCache.remove(indexNode);
            if (indexNodeCache.isEmpty()) {
                keyCode2IndexNodeListMap.remove(indexNode.getKey());
            }
        }
    }

    /**
     * 新增的节点在树的缓存中注册
     *
     * @param newIndexNodeList 新增的节点集合
     */
    private void registeIndexNodeList(List<CollectionKeyTreeMapNode> newIndexNodeList) {
        if (CollectionUtils.isNotEmpty(newIndexNodeList)) {
            for (CollectionKeyTreeMapNode newIndexNode : newIndexNodeList) {
                registeIndexNode(newIndexNode);
            }
        }
    }

    private void registeIndexNode(CollectionKeyTreeMapNode<E, T> newIndexNode) {
        if (newIndexNode != null) {
            E nodeKey = newIndexNode.getKey();
            CollectionKeyTreeMapNodeCache indexNodeCache = keyCode2IndexNodeListMap.get(nodeKey);
            if (indexNodeCache == null) {
                indexNodeCache = new CollectionKeyTreeMapNodeCache(nodeKey);
                keyCode2IndexNodeListMap.put(nodeKey, indexNodeCache);
            }
            indexNodeCache.add(newIndexNode);
        }
    }

    /**
     * 对keyCodeSet进行排序后返回
     * 按照keycode从小到大排序
     *
     * @param keyCodeSet
     * @return
     */
    private List<E> getSortKeyList(Collection<E> keyCodeSet) {
        List<E> sortkeyList = new ArrayList<>(keyCodeSet);
        sortkeyList.sort(Comparator.comparing(Object::toString));
        return sortkeyList;
    }

    public class CollectionKeyTreeMapNodeCache<E> {

        private E key;

        private ListMultimap<E, CollectionKeyTreeMapNode> key2CollectionKeyTreeMapNodeCache;

        protected CollectionKeyTreeMapNodeCache(E keyKey) {
            this.key = keyKey;
            init();
        }

        protected void init() {
            key2CollectionKeyTreeMapNodeCache = ArrayListMultimap.create();
        }

        protected void add(CollectionKeyTreeMapNode indexNode) {
            if (indexNode != null) {
                Set<E> keySet = indexNode.getKeySet();
                for (E key : keySet) {
                    key2CollectionKeyTreeMapNodeCache.put(key, indexNode);
                }
            }
        }

        protected void remove(CollectionKeyTreeMapNode indexNode) {
            if (indexNode != null) {
                Set<E> keySet = indexNode.getKeySet();
                for (E key : keySet) {
                    key2CollectionKeyTreeMapNodeCache.remove(key, indexNode);
                }
            }
        }

        protected List<CollectionKeyTreeMapNode> getSimilarIndexNode(Collection<E> keySet) {
            if (CollectionUtils.isEmpty(keySet)) {
                return null;
            }
            List<CollectionKeyTreeMapNode> rlt = null;
            Set<E> keySetCopy = new HashSet<>(keySet);
            for (E key : keySetCopy) {
                List<CollectionKeyTreeMapNode> keyRlt = key2CollectionKeyTreeMapNodeCache.get(key);
                if (rlt == null || keyRlt.size() < rlt.size()) {
                    rlt = keyRlt;
                }
            }
            return rlt;
        }

        private boolean isEmpty() {
            return key2CollectionKeyTreeMapNodeCache.isEmpty();
        }

    }

    public class CollectionKeyTreeMapNode<E, T> {

        private CollectionKeyTreeMap tree;

        private E key;

        private Set<E> keySet;

        private CollectionKeyTreeMapNode fatherNode;

        private Map<E, CollectionKeyTreeMapNode<E, T>> keyCode2childNodeMap = new HashMap<>();

        private T data;

        /**
         * 用于根节点的创建
         *
         * @param tree
         */
        protected CollectionKeyTreeMapNode(CollectionKeyTreeMap tree) {
            keySet = new HashSet<>();
            this.tree = tree;
        }

        /**
         * 用于非叶子节点的创建
         *
         * @param tree
         * @param key
         * @param keySet
         * @param fatherNode
         */
        protected CollectionKeyTreeMapNode(CollectionKeyTreeMap tree, CollectionKeyTreeMapNode fatherNode,
                                           E key, Set<E> keySet) {
            this.tree = tree;
            this.key = key;
            this.keySet = keySet;
            this.fatherNode = fatherNode;
        }

        /**
         * 用于叶子节点的创建
         *
         * @param tree
         * @param key
         * @param keySet
         * @param data
         * @param fatherNode
         */
        protected CollectionKeyTreeMapNode(CollectionKeyTreeMap tree, CollectionKeyTreeMapNode fatherNode,
                                           E key, Set<E> keySet, T data) {
            this.tree = tree;
            this.key = key;
            this.keySet = keySet;
            this.data = data;
            this.fatherNode = fatherNode;
        }

        /**
         * 向当前节点添加一个data
         * <p>
         * 如果data对应的keyList已经在节点树种存在则会返回false
         *
         * @param keyList data的key集合
         * @param index   已经遍历到keyList的第几个元素
         * @param data
         * @return 本次创建的所有节点集合
         */
        protected List<CollectionKeyTreeMapNode> addkeyCollectionNode(List<E> keyList, int index, T data) {
            List<CollectionKeyTreeMapNode> rlt = new ArrayList<>();
            if (CollectionUtils.isEmpty(keyList) || index >= keyList.size()) {
                return rlt;
            }

            E key = keyList.get(index);
            CollectionKeyTreeMapNode childNode = keyCode2childNodeMap.get(key);

            if (keyList.size() == index + 1) {
                //如果index+1等于keyList.size()，则说明已经遍历到最后一个key了，需要增加叶子节点来存储data
                //如果childNode == null，则新增一个叶子节点
                if (childNode == null) {
                    Set<E> newChildNodekeySet = new HashSet<>(keySet);
                    newChildNodekeySet.add(key);
                    CollectionKeyTreeMapNode newChildNode = new CollectionKeyTreeMapNode(tree, this, key, newChildNodekeySet, data);
                    addChild0(newChildNode);
                    rlt.add(newChildNode);
                } else {
                    //如果childNode != null，直接修改data数据
                    childNode.data = data;
                }
            } else {
                //如果还没到最后一个key，则继续想当前节点的孩子节点遍历
                if (childNode == null) {
                    //如果当前遍历的key没有在当前节点下找到孩子，则新增当前key对应非叶子节点的孩子
                    Set<E> newChildNodekeySet = new HashSet<>(keySet);
                    newChildNodekeySet.add(key);
                    childNode = new CollectionKeyTreeMapNode(tree, this, key, newChildNodekeySet);
                    addChild0(childNode);
                    rlt.add(childNode);
                }
                //向孩子中插入
                List<CollectionKeyTreeMapNode> childRlt = childNode.addkeyCollectionNode(keyList, index + 1, data);
                rlt.addAll(childRlt);
            }
            return rlt;
        }

        private void addChild0(CollectionKeyTreeMapNode<E, T> newChildNode) {
            keyCode2childNodeMap.put(newChildNode.getKey(), newChildNode);
        }

        /**
         * 查找当前节点下所有指定集合的子集的数据并返回
         *
         * @param fatherCollection 父集
         * @return 子集集合
         */
        protected List<T> getAllChildCollection(List<E> fatherCollection) {
            List<T> rlt = new ArrayList<>();
            //如果当前是带数据的节点则返回data数据
            if (data != null) {
                rlt.add(data);
            }
            if (CollectionUtils.isNotEmpty(fatherCollection)) {
                //如果不是叶子节点则遍历当前节点的孩子节点
                for (int index = 0; index < fatherCollection.size(); index++) {

                    E key = fatherCollection.get(index);
                    CollectionKeyTreeMapNode childNode = keyCode2childNodeMap.get(key);

                    if (childNode != null) {
                        List<E> newFatherCollection = fatherCollection.subList(index + 1, fatherCollection.size());
                        rlt.addAll(childNode.getAllChildCollection(newFatherCollection));
                    }
                }
            }

            return rlt;
        }

        /**
         * 查找一个指定集合的父集的数据并返回
         *
         * @param childCollection
         * @return 父集包含的数据（如果找不到则返回null）
         */
        protected T getOneFatherCollection(List<E> childCollection) {
            //判断当前的keySet是childCollection的父集
            if (keySet.containsAll(childCollection)) {
                return getOnekeyCollectionNode();
            } else {
                return null;
            }
        }

        /**
         * 获取当前节点下的任意一个keyCollectionNode并返回
         *
         * @return
         */
        private T getOnekeyCollectionNode() {
            //如果当前节点有数据则返回data
            if (data != null) {
                return data;
            } else {
                //如果没有数据则递归孩子节点获取data
                List<CollectionKeyTreeMapNode<E, T>> childNodeList = new ArrayList<>(keyCode2childNodeMap.values());
                for (CollectionKeyTreeMapNode<E, T> childNode : childNodeList) {
                    T data = childNode.getOnekeyCollectionNode();
                    if (data != null) {
                        return data;
                    }
                }
            }
            return null;
        }


        /**
         * 删除指定key集合的节点
         *
         * @param keyList
         * @return 返回删除的节点集合（如果返回集合为空则删除失败）
         */
        protected List<CollectionKeyTreeMapNode> removekeyCollectionNode(List<E> keyList) {
            List<CollectionKeyTreeMapNode> rlt = new ArrayList<>();
            if (CollectionUtils.isEmpty(keyList)) {
                return rlt;
            }

            E nextChildKey = keyList.get(0);
            CollectionKeyTreeMapNode nextChildNode = keyCode2childNodeMap.get(nextChildKey);

            if (nextChildNode != null) {

                if (keyList.size() == 1) {
                    //如果是叶子节点则删除
                    if (nextChildNode.isDataLeafNode()) {
                        CollectionKeyTreeMapNode removeChildeNode = removeChildeNode(nextChildKey);
                        rlt.add(removeChildeNode);
                    } else {
                        nextChildNode.data = null;
                    }


                } else if (keyList.size() > 1) {

                    List<CollectionKeyTreeMapNode> childNodeRemoveRlt = nextChildNode.removekeyCollectionNode(keyList.subList(1, keyList.size()));
                    if (CollectionUtils.isNotEmpty(childNodeRemoveRlt)) {
                        rlt.addAll(childNodeRemoveRlt);
                        if (nextChildNode.isLeafNode() && nextChildNode.data == null) {
                            CollectionKeyTreeMapNode removeChildeNode = removeChildeNode(nextChildKey);
                            rlt.add(removeChildeNode);
                        }
                    }

                }

            }
            return rlt;
        }

        /**
         * 删除一个指定key的孩子节点
         *
         * @param removeChildeNodekey
         * @return
         */
        private CollectionKeyTreeMapNode removeChildeNode(E removeChildeNodekey) {
            CollectionKeyTreeMapNode removeChildeNode = keyCode2childNodeMap.remove(removeChildeNodekey);
            return removeChildeNode;
        }


        protected List<T> getAllData() {
            List<T> rlt = new ArrayList<>();

            if (data != null) {
                rlt.add(data);
            }
            //如果不是叶子节点，则继续向下遍历
            if (!isLeafNode()) {
                Collection<CollectionKeyTreeMapNode<E, T>> childNodes = keyCode2childNodeMap.values();
                for (CollectionKeyTreeMapNode<E, T> childNode : childNodes) {
                    rlt.addAll(childNode.getAllData());
                }
            }

            return rlt;
        }

        /**
         * 判断当前节点是否是叶子节点
         *
         * @return
         */
        protected boolean isLeafNode() {
            return keyCode2childNodeMap.isEmpty();
        }

        /**
         * 判断当前节点是否是存储数据的叶子节点
         *
         * @return
         */
        protected boolean isDataLeafNode() {
            return isLeafNode() && data != null;
        }


        protected E getKey() {
            return key;
        }

        protected Set<E> getKeySet() {
            return new HashSet<>(keySet);
        }

        protected T getData() {
            return data;
        }

    }

    public static void main(String[] args) throws IOException {
        test3();
    }

    private static void test() throws IOException {
        CollectionKeyTreeMap<String, Set<String>> tree = new CollectionKeyTreeMap();

        Set<String> skuSet1 = new HashSet<>();
        skuSet1.add("sku1");
        skuSet1.add("sku3");
        skuSet1.add("sku7");
        skuSet1.add("sku2");
        Set<String> skuSet2 = new HashSet<>();
        skuSet2.add("sku1");
        skuSet2.add("sku4");
        skuSet2.add("sku6");
        skuSet2.add("sku2");
        Set<String> skuSet3 = new HashSet<>();
        skuSet3.add("sku8");
        skuSet3.add("sku3");
        skuSet3.add("sku7");
        skuSet3.add("sku2");
        Set<String> skuSet4 = new HashSet<>();
        skuSet4.add("sku3");
        skuSet4.add("sku7");
        skuSet4.add("sku5");
        Set<String> skuSet5 = new HashSet<>();
        skuSet5.add("sku3");
        skuSet5.add("sku5");
        Set<String> skuSet6 = new HashSet<>();
        skuSet6.add("sku3");
        skuSet6.add("sku7");

        tree.put(skuSet1, skuSet1);
        tree.put(skuSet2, skuSet2);
        tree.put(skuSet3, skuSet3);
        tree.put(skuSet4, skuSet4);
        tree.put(skuSet5, skuSet5);
        tree.put(skuSet6, skuSet6);

        System.out.println("getAllData:" + tree.getValues());

        //查找fatherCollection的所有子集
        Set<String> fatherCollection = new HashSet<>();
        fatherCollection.add("sku1");
        fatherCollection.add("sku2");
        fatherCollection.add("sku3");
        fatherCollection.add("sku7");
        fatherCollection.add("sku5");
        fatherCollection.add("sku8");
        List<Set<String>> allChildCollection = tree.getAllChildCollection(fatherCollection);
        System.out.println(allChildCollection);
        //查找childCollection的一个父集
        Set<String> childCollection = new HashSet<>();
        childCollection.add("sku1");
        childCollection.add("sku2");
        childCollection.add("sku3");
        Set<String> oneFatherCollection = tree.getOneFatherCollection(childCollection);
        System.out.println(oneFatherCollection);
        tree.remove(skuSet2);
        tree.remove(skuSet3);
    }

    private static void test3() throws IOException {
        CollectionKeyTreeMap<String, Set<String>> tree = new CollectionKeyTreeMap();

        Set<String> skuSet1 = new HashSet<>();
        skuSet1.add("sku1");
        skuSet1.add("sku3");
        skuSet1.add("sku7");
        skuSet1.add("sku2");
        Set<String> skuSet2 = new HashSet<>();
        skuSet2.add("sku1");
        skuSet2.add("sku2");

        tree.put(skuSet1, skuSet1);
        tree.put(skuSet2, skuSet2);

        System.out.println("getAllData:" + tree.getValues());

        tree.remove(skuSet2);
        System.out.println("getAllData:" + tree.getValues());
    }

    private static void test2() throws IOException {
        Set<Set<String>> keySetList = getKeySets();

        System.out.println(keySetList.size());

        List<Set<String>> findChilds = new ArrayList<>();
        Long createTreeStarTiem = System.currentTimeMillis();
        CollectionKeyTreeMap<String, Set<String>> tree = new CollectionKeyTreeMap();

        for (Set<String> stringSet : keySetList) {
            List<Set<String>> allChildCollection = tree.getAllChildCollection(stringSet);
            Set<String> oneFatherCollection = tree.getOneFatherCollection(stringSet);

            if (oneFatherCollection != null) {
                findChilds.add(stringSet);
            } else if (CollectionUtils.isNotEmpty(allChildCollection)) {
                findChilds.addAll(allChildCollection);
                allChildCollection.forEach(key -> tree.remove(key));
                tree.put(stringSet, stringSet);
            } else {
                tree.put(stringSet, stringSet);
            }
        }
        System.out.println("run time : " + (System.currentTimeMillis() - createTreeStarTiem) + "ms");
        List<Set<String>> values = tree.getValues();
        System.out.println(tree.getValues().size());
        System.out.println("findChilds：" + findChilds.size());
        boolean success = true;
        //校验结果集中是否有父子集关系
        createTreeStarTiem = System.currentTimeMillis();
        A:
        for (int i = 0; i < values.size(); i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            Set<String> stringSet = values.get(i);
            for (int j = i + 1; j < values.size(); j++) {
                Set<String> stringSet2 = values.get(j);
                if (stringSet.containsAll(stringSet2) || stringSet2.containsAll(stringSet)) {
                    success = false;
                    break A;
                }
            }
        }
        System.out.println("run time : " + (System.currentTimeMillis() - createTreeStarTiem) + "ms");
        System.out.println(success);

        //校验子集中能否在结果集中找到父集
        success = true;
        createTreeStarTiem = System.currentTimeMillis();
        for (int i = 0; i < findChilds.size(); i++) {
            if (i % 1000 == 0) {
                System.out.println(i);
            }
            Set<String> stringSet = findChilds.get(i);
            boolean findfather = false;
            for (int j = 0; j < values.size(); j++) {
                Set<String> stringSet2 = values.get(j);
                if (stringSet2.containsAll(stringSet)) {
                    findfather = true;
                    break;
                }
            }
            if (!findfather) {
                Set<String> oneFatherCollection = tree.getOneFatherCollection(stringSet);
                System.out.println(oneFatherCollection);
                System.out.println(stringSet);
                System.out.println(oneFatherCollection.containsAll(stringSet));
                success = false;
                break;
            }
        }
        System.out.println("run time : " + (System.currentTimeMillis() - createTreeStarTiem) + "ms");
        System.out.println(success);

    }

    private static Set<Set<String>> getKeySets() {
        Set<Set<String>> skuSetList = new HashSet<>(50000);
        for (int i = 0; i < 30000; i++) {
            int skuSetSize = (int) (Math.random() * 9 + 1);
            Set<String> skuSet = new HashSet<>();
            for (int j = 0; j < skuSetSize; j++) {
                int skuCode = (int) (Math.random() * 1000);
                skuSet.add(String.valueOf(skuCode));
            }
            skuSetList.add(skuSet);
        }
        return skuSetList;
    }
}
