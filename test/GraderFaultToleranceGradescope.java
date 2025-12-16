import com.gradescope.jh61b.grader.GradedTestListenerJSON;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import com.gradescope.jh61b.grader.GradedTest;


import java.io.IOException;
import java.lang.reflect.Method;

public class GraderFaultToleranceGradescope {
public static void main(String[] args) throws IOException {

	Class<?> testClass = GraderFaultTolerance.class;
	Method[] methods = testClass.getDeclaredMethods();

	for (Method method : methods) {
		if (method.isAnnotationPresent(GradedTest.class)) {
			GradedTest annotation = method.getAnnotation(GradedTest.class);
			double maxScore = annotation.max_score();
			System.out.println("TestNameAndScore: testName=" + method.getName() + " " + "max_score" + "=" + maxScore);
		}
	}

	JUnitCore runner = new JUnitCore();
	runner.addListener(new GradedTestListenerJSON());
	Result r = runner.run(GraderFaultTolerance.class);
}
}
