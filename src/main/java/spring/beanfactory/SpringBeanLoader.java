package spring.beanfactory;

import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Spring Bean工厂加载类
 * @author cxp
 *
 */
public class SpringBeanLoader {
	
	private AbstractApplicationContext context;
	
	public SpringBeanLoader(){
		this("application-context.xml",null);
	}
	
	public SpringBeanLoader(String configFilePath,Class contextClass){
		//启动spring bean管理器
		if(contextClass==null){
			context = new ClassPathXmlApplicationContext(configFilePath);
		}else{
			context=new ClassPathXmlApplicationContext(configFilePath,contextClass);
		}
		context.registerShutdownHook();
	}
	
	/**
	 * 从Spring Bean工厂中获取Bean
	 * @param requiredType
	 * @return
	 */
	public <T> T getBean(Class<T> requiredType){
		return context.getBean(requiredType);
	}
}
