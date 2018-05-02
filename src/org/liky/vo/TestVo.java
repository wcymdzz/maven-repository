package org.liky.vo;

public class TestVo {
private int id;
private String title;
private String url;
public TestVo(int id, String title, String url) {
	super();
	this.id = id;
	this.title = title;
	this.url = url;
}
public TestVo() {
	super();
}
public int getId() {
	return id;
}
public void setId(int id) {
	this.id = id;
}
public String getTitle() {
	return title;
}
public void setTitle(String title) {
	this.title = title;
}
public String getUrl() {
	return url;
}
public void setUrl(String url) {
	this.url = url;
};

}
