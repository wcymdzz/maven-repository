package org.liky.dao;

import java.util.List;

import org.liky.vo.TestVo;

public interface TestDAO {
public List<TestVo> findAll()throws Exception;
public List<TestVo> findByKeyword(String keyword)throws Exception;
}
