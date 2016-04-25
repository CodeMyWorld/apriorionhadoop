package MapReduce;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

//每个map运行出局部频繁项
public class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
	private double minSupport = 0.2;//最小支持度
	private int minSup = 0;
	private int count = 0; //记录事物数目
	private int[] supportArr;
	private int currentItemCount; //记录当前第几项
	private final int itemCount = 3; //设置迭代到第几项
	
	List<String> dataList = new ArrayList<String>();
    List<Set<String>> dataSet = new ArrayList<Set<String>>();
	//利用map方法得到1项集
	public void map(LongWritable key, Text value, Context context) throws IOException{
		dataList.add(value.toString());
		Set<String>dSet = new TreeSet<String>();
		String[] dArr = value.toString().split(" ");
		for(String str : dArr){
			dSet.add(str);
		}
		dataSet.add(dSet);
		count++;
	}
	
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException{
		minSup = (int)minSupport * count;
		List<Set<String>> resualt = FindF1Item(dataList);
		
		for(currentItemCount = 2; currentItemCount <= itemCount; currentItemCount++){
			resualt = arioriGen(resualt);
		}
		
		int indexOfSupport = 0;
		for(Set<String>set : resualt){
			StringBuffer itemBuffer = new StringBuffer();
			for(String str : set){
				itemBuffer.append(str+" ");
			}
			context.write(new Text(itemBuffer.toString()), new IntWritable(supportArr[indexOfSupport]));
			indexOfSupport++;
		}
		
	}
	
	/**
	 * 找出候选1项集 这里是找出频繁一项集
	 * 
	 * @param data
	 * @return result
	 * 扫描所有事务项，将全部已项集存储到result
	 */
	List<Set<String>> FindF1Item(List<String> data) {
		List<Set<String>> result = new ArrayList<Set<String>>();
		Map<String, Integer> dc = new HashMap<String, Integer>();
		for (String d : data) {
			String[] items = d.split(" ");
			for (String item : items) {
				if (dc.containsKey(item)) {
					dc.put(item, dc.get(item) + 1);
				} else {
					dc.put(item, 1);
				}
			}
		}
		Set<String> itemKeys = dc.keySet(); //I1 I2 I3这些
		Set<String> tempKeys = new TreeSet<String>();
		for (String str : itemKeys) {
			tempKeys.add(str);
		}

		for (String item : tempKeys) {
			if (dc.get(item) >= minSup) {
				Set<String> f1Set = new TreeSet<String>();
				f1Set.add(item);
				result.add(f1Set);
			}
		}
		return result;
	}
	
	/**
	 * 利用arioriGen方法由k-1项集生成k项集
	 * 
	 * @param preSet
	 * @return
	 * 
	 */
	List<Set<String>> arioriGen(List<Set<String>> preSet) {

		List<Set<String>> result = new ArrayList<Set<String>>();
		int preSetSize = preSet.size();

		for (int i = 0; i < preSetSize - 1; i++) {
			for (int j = i + 1; j < preSetSize; j++) {
				String[] strA1 = preSet.get(i).toArray(new String[0]);
				String[] strA2 = preSet.get(j).toArray(new String[0]);
				if (isCanLink(strA1, strA2)) {// 判断两个k-1项集是否符合连接成K项集的条件
					Set<String> set = new TreeSet<String>();
					for (String str : strA1) {
						set.add(str);// 将strA1加入set中连成前K-1项集
					}
					set.add((String) strA2[strA2.length - 1]);// 连接成K项集
					// 判断K项集是否需要剪切掉，如果不需要被cut掉，则加入到k项集的列表中
					if (!isNeedCut(preSet, set)) {
						result.add(set);
					}
				}
			}
		}
		return checkSupport(result);// 返回的都是频繁K项集
	}
	
	/**
	 * 把set中的项集与数量集比较并进行计算，求出支持度大于要求的项集
	 * 
	 * @param set
	 * @return
	 */
	List<Set<String>> checkSupport(List<Set<String>> setList) {
    
		List<Set<String>> result = new ArrayList<Set<String>>();
		boolean flag = true;
		int[] counter = new int[setList.size()];
		for (int i = 0; i < setList.size(); i++) {

			for (Set<String> dSets : dataSet) {
				if (setList.get(i).size() > dSets.size()) {
					flag = true;
				} else {
					for (String str : setList.get(i)) {  //如果此项没有出现在数据集中
						if (!dSets.contains(str)) {
							flag = false;
							break;
						}
					}
					if (flag) {
						counter[i] += 1;
					} else {
						flag = true;
					}
				}
			}
		}

		for (int i = 0; i < setList.size(); i++) {
			if (counter[i] >= minSup) {
				result.add(setList.get(i));
			}
		}
		if(currentItemCount == itemCount){
		supportArr = counter; //如果是最后一次迭代，将支持度读取出来
		}
		return result;
	}
	
	/**
	 * 判断两个项集能否执行连接操作
	 * 
	 * @param s1
	 * @param s2
	 * @return
	 */
	boolean isCanLink(String[] s1, String[] s2) {
		boolean flag = true;    //4项前3项要一样并且最后一项不一样，就可以连接 前n-1项一样 最后一项不一样
		if (s1.length == s2.length) {
			for (int i = 0; i < s1.length - 1; i++) {
				if (!s1[i].equals(s2[i])) {
					flag = false;
					break;
				}
			}
			if (s1[s1.length - 1].equals(s2[s2.length - 1])) {
				flag = false;
			}
		} else {
			flag = true;
		}
		return flag;
	}

	/**
	 * 判断set是否需要被cut
	 * 
	 * @param setList
	 * @param set
	 * @return
	 */
	boolean isNeedCut(List<Set<String>> setList, Set<String> set) {// setList指频繁K-1项集，set指候选K项集
		boolean flag = false;
		List<Set<String>> subSets = getSubset(set);// 获得K项集的所有k-1项集
		for (Set<String> subSet : subSets) {
			// 判断当前的k-1项集set是否在频繁k-1项集中出现，如果出现，则不需要cut
			// 若没有出现，则需要被cut
			if (!isContained(setList, subSet)) {
				flag = true;
				break;
			}
		}
		return flag;
	}

	/**
	 * 功能:判断k项集的某k-1项集是否包含在频繁k-1项集列表中
	 * 
	 * @param setList
	 * @param set
	 * @return
	 */
	boolean isContained(List<Set<String>> setList, Set<String> set) {
		boolean flag = false;
		int position = 0;
		for (Set<String> s : setList) {
			String[] sArr = s.toArray(new String[0]);
			String[] setArr = set.toArray(new String[0]);
			for (int i = 0; i < sArr.length; i++) {
				if (sArr[i].equals(setArr[i])) {
					// 如果对应位置的元素相同，则position为当前位置的值
					position = i;
				} else {
					break;
				}
			}
			// 如果position等于数组的长度，说明已经找到某个setList中的集合与
			// set集合相同了，退出循环，返回包含
			// 否则，把position置为0进入下一个比较
			if (position == sArr.length - 1) {
				flag = true;
				break;
			} else {
				flag = false;
				position = 0;
			}
		}
		return flag;
	}

	/**
	 * 获得k项集的所有k-1项子集
	 * 
	 * @param set
	 * @return
	 */
	List<Set<String>> getSubset(Set<String> set) {

		List<Set<String>> result = new ArrayList<Set<String>>();
		String[] setArr = set.toArray(new String[0]);

		for (int i = 0; i < setArr.length; i++) {
			Set<String> subSet = new TreeSet<String>();
			for (int j = 0; j < setArr.length; j++) {
				if (i != j) {
					subSet.add((String) setArr[j]);
				}
			}
			result.add(subSet);
		}
		return result;
	}

}
