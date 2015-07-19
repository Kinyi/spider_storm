package cn.usth.utils;



/**
 * 商品实体类
 * 
 *
 */
public class Goods {
	private String id;
	/**
	 * 商品名称
	 */
	private String name;

	/**
	 * 价格
	 */
	private float price;

	/**
	 * 图片url
	 * 
	 */
	private String picpath;

	/**
	 * 商品链接
	 * 
	 */
	private String dataurl;
	/**
	 * 来源
	 * 
	 */
	private String from;
	
	/**
	 * 规格参数
	 */
	private String spec;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public String getPicpath() {
		return picpath;
	}

	public void setPicpath(String picpath) {
		this.picpath = picpath;
	}

	public String getDataurl() {
		return dataurl;
	}

	public void setDataurl(String dataurl) {
		this.dataurl = dataurl;
	}
	
	
	public String getSpec() {
		return spec;
	}

	public void setSpec(String spec) {
		this.spec = spec;
	}
	
	public String getFrom() {
		return from;
	}

	public void setFrom(String from) {
		this.from = from;
	}

	@Override
	public String toString() {
		return "Goods [id=" + id + ", name=" + name + ", price=" + price
				+ ", picpath=" + picpath + ", dataurl=" + dataurl + ", from="
				+ from + ", spec=" + spec + "]";
	}

	
	

}
