package com.mob.dataengine.commons.annotation

/**
 * @author juntao zhang
 */
package object code {
  class author(creatorName: String = "") extends scala.annotation.StaticAnnotation
  /* 业务名称 */
  class businessName(msg: String = "") extends scala.annotation.StaticAnnotation
  /* 项目名称 */
  class projectName(msg: String = "") extends scala.annotation.StaticAnnotation
  /* 创建时间 */
  class createTime(msg: String = "") extends scala.annotation.StaticAnnotation
  /* ","分割 */
  class sourceTable(msg: String = "") extends scala.annotation.StaticAnnotation
  /* ","分割 */
  class targetTable(msg: String = "") extends scala.annotation.StaticAnnotation
  /* 解释备注 */
  class explanation(msg: String = "") extends scala.annotation.StaticAnnotation
}
