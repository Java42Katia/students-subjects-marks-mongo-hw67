package telran.college;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import telran.college.dto.*;
import telran.college.service.CollegeService;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class StudentsSubjectsMarksMongoApplicationTests {
@Autowired
	CollegeService collegeService;
	@Test
	@Order(1)
	void addMarks() {
		collegeService.addMark(new Mark(1, 1, 70));
		collegeService.addMark(new Mark(1, 1, 80));
		collegeService.addMark(new Mark(1, 1, 90));
		collegeService.addMark(new Mark(2, 1, 75));
		collegeService.addMark(new Mark(2, 1, 80));
		collegeService.addMark(new Mark(3, 1, 60));
		
		collegeService.addMark(new Mark(4, 2, 65));
		collegeService.addMark(new Mark(5, 3, 70));
		
	}
	@Test
	@Order(2)
	void getMarksStudentSubjectTest() {
		List<Integer> expected = Arrays.asList(70, 80, 90);
		List<Integer> actual = collegeService.getStudentMarksSubject("student1", "subject1");
		
		assertIterableEquals(expected, actual);
		collegeService.addMark(new Mark(3, 2, 60));
		expected = Arrays.asList(60);
		actual = collegeService.getStudentMarksSubject("student3", "subject1");
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order(3)
	void getStudentsSubjectMarks() {
		List<String> expected = Arrays.asList("student1", "student2");
		List<String> actual = collegeService.getStudentsSubjectMark("subject1", 70);
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order(4)
	void getGoodStudents() {
		List<Student> expected = Arrays.asList(new Student(1, "student1"), new Student(2, "student2"));
		List<Student> actual = collegeService.goodCollegeStudents();
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order(14)
	void deleteStudents() {
		collegeService.deleteStudentsAvgMarkLess(70);
		List<String> actual = collegeService.getStudentsSubjectMark("subject1", 30);
		assertEquals(2, actual.size());
		
	}
	@Test
	@Order(6)
	void getSubjectsAvgMarkGreater() {
		List<Subject> expected = Arrays.asList(new Subject(1, "subject1"), new Subject(3, "subject3"));
		List<Subject> actual = collegeService.subjectsAvgMarkGreater(65);
		assertIterableEquals(expected, actual);
		
	}
	@Test
	@Order(15)
	void deleteStudentsCountLess() {
		List<Student> expected = Arrays.asList(new Student(5, "student5"));
		List<Student> actual = collegeService.deleteStudentsMarksCountLess(2);
		
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order(5)
	void bestStudents() {
		List<Student> expected = Arrays.asList(new Student(1, "student1"));
		List<Student> actual = collegeService.bestStudents(1);
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order (7)
	void studentsAllMarksSubjectGreaterEqual () {
		List<Student> expected = Arrays.asList(new Student(2, "student2"));
		List<Student> actual = collegeService.getStudentsAllMarksSubject(75, "subject1");
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order(8)
	void bestStudentsSubjects () {
		List<Student> expected = Arrays.asList(new Student(1, "student1"),new Student(2, "student2") );
		List<Student> actual = collegeService.bestStudentsSubject(2,"subject1");
		assertIterableEquals(expected, actual);
	}
	@Test
	@Order (9)
	void subjectWithGreatestAvgMark() {
		Subject expected = new Subject(1,  "subject1");
		assertEquals(expected, collegeService.subjectGreatestAvgMark());
	}
	@Test
	@Order (10)
	void studentsMaxMarks() {
		List<Student> expected = Arrays.asList(new Student(1, "student1"));
		assertIterableEquals(expected, collegeService.getStudentsMaxMarksCount());
	}
	@Test
	@Order (11)
	void subjectsAvgMarkLess() {
		Iterable<Subject> expected = Arrays.asList(new Subject(2, "subject2"),
				new Subject(4, "subject4"), new Subject(5, "subject5"));
		assertIterableEquals(expected , collegeService.getSubjectsAvgMarkLess(70));
	}
	
	

}

