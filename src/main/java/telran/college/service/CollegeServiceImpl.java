package telran.college.service;

import java.util.List;

import org.slf4j.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import telran.college.documents.*;
import telran.college.dto.*;
import telran.college.projection.*;
import telran.college.repo.*;
@Service
public class CollegeServiceImpl implements CollegeService {
	static Logger LOG = LoggerFactory.getLogger(CollegeService.class);
   StudentRepository studentsRepository;
   SubjectRepository subjectsRepository;
   
	public CollegeServiceImpl(StudentRepository studentsRepository, SubjectRepository subjectsRepository) {
	this.studentsRepository = studentsRepository;
	this.subjectsRepository = subjectsRepository;
}

	@Override
	@Transactional
	public void addStudent(Student student) {
		if (studentsRepository.existsById(student.id)) {
			throw new RuntimeException(String.format("student with id %d already exists", student.id));
		}
		StudentDoc studentDoc = new StudentDoc(student.id, student.name);
		studentsRepository.save(studentDoc);
		

	}

	@Override
	@Transactional
	public void addSubject(Subject subject) {
		if (subjectsRepository.existsById(subject.id)) {
			throw new RuntimeException(String.format("subject with id %d already exists", subject.id));
		}
		SubjectDoc subjectDoc = new SubjectDoc(subject.id, subject.subjectName);
		subjectsRepository.save(subjectDoc);

	}

	@Override
	@Transactional
	public void addMark(Mark mark) {
		StudentDoc studentDoc = studentsRepository.findById(mark.stid).orElse(null);
		if (studentDoc == null) {
			throw new RuntimeException(String.format("Student with id %d doesn't exist", mark.stid));
		}
		SubjectDoc subjectDoc = subjectsRepository.findById(mark.suid).orElse(null);
		if (subjectDoc == null) {
			throw new RuntimeException(String.format("Subject with id %d doesn't exist", mark.suid));
		}
		List<SubjectMark> marks = studentDoc.getMarks();
		marks.add(new SubjectMark(subjectDoc.getSubjectName(), mark.mark));
		studentsRepository.save(studentDoc);

	}

	@Override
	public List<String> getStudentsSubjectMark(String subjectName, int mark) {
		List<StudentNameProj> students = studentsRepository.findByMarksSubjectAndMarksMarkGreaterThanEqual(subjectName, mark);
		LOG.debug("students from getStudentsSubjectMark : {}", students);
		return students.stream().map(s -> s.getName()).toList();
	}

	@Override
	public List<Student> goodCollegeStudents() {
		
		return studentsRepository.findGoodStudents();
	}

	@Override
	public List<Student> bestStudents(int nStudents) {
		
		return studentsRepository.findTopBestStudents(nStudents);
	}


	/********************** hw 67 ****************/

	@Override
	public List<Integer> getStudentMarksSubject(String name, String subjectName) {
		
//		StudentMarksProj marks = studentsRepository.findByNameAndMarksSubject(name, subjectName);
//		LOG.debug("marks from getStudentMarksSubject : {} of student {}",  marks, name);
//		return marks.getMarks().stream().filter(sm -> sm.getSubject().equals(subjectName)).map(SubjectMark::getMark).toList();
		
		return studentsRepository.findStudentMarksSubject(name, subjectName);
	}

	@Override
	public List<Student> bestStudentsSubject(int nStudents, String subjectName) {
		
		return studentsRepository.findBestStudentsSubject(nStudents, subjectName);
	}

	@Override
	public Subject subjectGreatestAvgMark() {
		
		return studentsRepository.findSubjectGreatestAvgMark();
	}

	@Override
	public List<Subject> subjectsAvgMarkGreater(int avgMark) {
		
		return studentsRepository.findSubjectsAvgMarkGreater(avgMark);
	}

	@Override
	public void deleteStudentsAvgMarkLess(int avgMark) {
		
		studentsRepository.deleteStudentsAvgMarkLess(avgMark);
	}

	@Override
	public List<Student> deleteStudentsMarksCountLess(int count) {
		
		return studentsRepository.deleteStudentsMarksCountLess(count);
	}

	@Override
	public List<Student> getStudentsAllMarksSubject(int mark, String subject) {

		return studentsRepository.findStudentsAllMarksSubjectGreaterThen(mark, subject);
	}

	@Override
	public List<Student> getStudentsMaxMarksCount() {

		return studentsRepository.findStudentsMaxMarksCount();
	}

	@Override
	public List<Subject> getSubjectsAvgMarkLess(int avgMark) {

		return studentsRepository.findSubjectsAvgMarkLess(avgMark);
	}

}
