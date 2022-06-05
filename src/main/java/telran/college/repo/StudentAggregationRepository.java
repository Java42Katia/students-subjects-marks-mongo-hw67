package telran.college.repo;

import java.util.List;

import telran.college.dto.Student;
import telran.college.dto.Subject;

public interface StudentAggregationRepository {
List<Student> findTopBestStudents(int nStudents);
List<Student> findGoodStudents();

/************************** hw 67 ***************************/
List<Integer> findStudentMarksSubject(String name, String subjectName);
List<Student> findBestStudentsSubject(int nStudents, String subjectName);
List<Student> findStudentsAllMarksSubjectGreaterThen(int mark, String subject);
List<Student> findStudentsMaxMarksCount();
void deleteStudentsAvgMarkLess(int avgMark);
List<Student> deleteStudentsMarksCountLess(int count);
Subject findSubjectGreatestAvgMark();
List<Subject> findSubjectsAvgMarkGreater(int avgMark);
List<Subject> findSubjectsAvgMarkLess(int avgMark);
}
