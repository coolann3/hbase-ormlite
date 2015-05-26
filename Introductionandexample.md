# Introduction #

**ORM-HBase**

This is simple orm for HBase, the project includes several parts:

**DataMapper**

According to PoEAA, domain object knows nothing about Data Mapper, but mapper knows the domain object. Actually data mapper is designed for specific objet including functions to access databases.

**From PoEAA**
Here, the ORM-HBase is designed based on the model, but make it more general. We specify the mapping from object’s fields to HBase schema through annotation, and design a general DataMapper based on generic data type (with annotations).

**Object Beans**
Make sure the object has an empty construction function with no parameters and getter&setter functions for each member variable;

**Dao:**
User does not need to call Create() function of DataMapperFactory nor need to know anything about DataMapper’s functions. Only need to use Dao to do CRUD.


---



# Details #

**Bean**
package com.wlu.orm.hbase.tests;

import com.wlu.orm.hbase.annotation.DatabaseField;
import com.wlu.orm.hbase.annotation.DatabaseTable;

@DatabaseTable(tableName = "ResumeArchive")
public class Resume {
> @DatabaseField(id = true)
> String id;
> @DatabaseField(familyName = "resume\_title", qualifierName = "resume\_title")
> String resume\_title;
> @DatabaseField(familyName = "user\_basic\_information")
> Resume\_BasicInfo basicInfo;
> @DatabaseField(familyName = "user\_education")
> Resume\_Education education;
> @DatabaseField(familyName = "user\_work\_experience")
> Resume\_WorkExperience workExperience;

> public Resume() {
> > super();

> }

> public String getId() {
> > return id;

> }

> public void setId(String id) {
> > this.id = id;

> }

> public String getResume\_title() {
> > return resume\_title;

> }

> public void setResume\_title(String resume\_title) {
> > this.resume\_title = resume\_title;

> }

> public Resume\_BasicInfo getBasicInfo() {
> > return basicInfo;

> }

> public void setBasicInfo(Resume\_BasicInfo basicInfo) {
> > this.basicInfo = basicInfo;

> }

> public Resume\_Education getEducation() {
> > return education;

> }

> public void setEducation(Resume\_Education education) {
> > this.education = education;

> }

> public Resume\_WorkExperience getWorkExperience() {
> > return workExperience;

> }

> public void setWorkExperience(Resume\_WorkExperience workExperience) {
> > this.workExperience = workExperience;

> }

> @Override
> public String toString() {
> > return "Resume [id=" + id + "\n resume\_title=" + resume\_title
> > > + "\n basicInfo=" + basicInfo + "\n education=" + education
> > > + "\n workExperience=" + workExperience + "]";

> }

}


**Test Case**
package com.wlu.orm.hbase.tests;

import java.util.ArrayList;
import java.util.List;


import com.wlu.orm.hbase.connection.HBaseConnection;
import com.wlu.orm.hbase.dao.Dao;
import com.wlu.orm.hbase.dao.DaoImpl;
import com.wlu.orm.hbase.exceptions.HBaseOrmException;
import com.wlu.orm.hbase.schema.value.ValueFactory;

import junit.framework.TestCase;

public class ResumeTest extends TestCase {

> static HBaseConnection hbaseconnection = null;
> static Dao

&lt;Resume&gt;

 dao = null;
> static {
> > hbaseconnection = new HBaseConnection("wlu-hadoop01", "2181", 10);
> > try {
> > > dao = new DaoImpl

&lt;Resume&gt;

(Resume.class, hbaseconnection);

> > } catch (HBaseOrmException e) {
> > > e.printStackTrace();

> > }

> }

> public void testCreateTable() {
> > dao.CreateTableIfNotExist();

> }

> public void testInsert() {
> > Resume\_BasicInfo jacky\_b = new Resume\_BasicInfo();
> > jacky\_b.setSecond\_Name("Jacky");
> > jacky\_b.setFirst\_Name("Chen");
> > jacky\_b.setGender("Male");
> > jacky\_b.setData\_of\_Birth("1980-1-1");
> > jacky\_b.setEmail("jacky@sun.com");
> > jacky\_b.setResidency("DC");
> > jacky\_b.setYrs\_of\_Experience("5");


> Resume\_Education jacky\_e = new Resume\_Education();
> jacky\_e.setTime\_period("2000-6");
> jacky\_e.setSchool("Uta University");
> jacky\_e.setDegree("Master");
> jacky\_e.setMajor("Electronic Enginnering");

> Resume\_WorkExperience jacky\_w = new Resume\_WorkExperience();
> jacky\_w.setTime\_period("2006-3");
> jacky\_w.setCompany("Sun corp.");
> jacky\_w.setJobTitle("Software Enginner");
> jacky\_w.setDepartment("Data Service Team");
> jacky\_w.setDescription("I worked here for about 6 years and know a lot about ...");

> Resume jacky = new Resume();
> jacky.setId("00000001234");
> jacky.setResume\_title("Jacky's Personal Resume");
> jacky.setBasicInfo(jacky\_b);
> jacky.setEducation(jacky\_e);
> jacky.setWorkExperience(jacky\_w);

> dao.Insert(jacky);

> }

> public void testQuery() {
> > Resume jacky = dao.QueryById(ValueFactory.TypeCreate("00000001234"));
> > System.out.println(jacky);

> }

> public void testUpdate() {
> > Resume jacky = dao.QueryById(ValueFactory.TypeCreate("00000001234"));
> > System.out.println(jacky);
> > jacky.getBasicInfo().setTelephone\_number("010045087");
> > jacky.getBasicInfo()
> > > .setResidency("Wen er Road, #391, Hangzhou, China.");

> > jacky.getWorkExperience().setJobTitle("Senior Software Enginner");
> > List

&lt;String&gt;

 fl = new ArrayList

&lt;String&gt;

();
> > fl.add("basicInfo");
> > fl.add("workExperience");


> dao.Update(jacky, fl);
> System.out.println(jacky);
> }

> public void testDelete(){
> > dao.DeleteById(ValueFactory.TypeCreate("00000001234"));

> }
}

