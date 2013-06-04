package fr.sanofi.fcl4transmart.handlers;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.equinox.security.storage.ISecurePreferences;
import org.eclipse.equinox.security.storage.SecurePreferencesFactory;
import org.eclipse.equinox.security.storage.StorageException;
import org.eclipse.swt.SWT;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;

public class ProxyPreferencesHandler {
	private Shell shell;
	private Combo combo;
	private Composite body;
	private Text host;
	private Text port;
	private Text user;
	private Text pass;
	private Button authRequired;
	private static String staticMethod;
	private static String staticHost;
	private static String staticPort;
	private static boolean staticAuthRequired;
	private static String staticUser;
	private static String staticPass;
	private ISecurePreferences node;
	private Composite bodyPart;
	public ProxyPreferencesHandler(){
		try {
			ISecurePreferences securePref= SecurePreferencesFactory.getDefault();
	        node = securePref.node("proxy");
			staticMethod=node.get("method", "No proxy");
			staticAuthRequired=false;
        
            staticHost=node.get("host", "");
            staticPort=node.get("port", "");
            staticAuthRequired=node.getBoolean("authRequired", false);
            if(staticAuthRequired){
            	staticUser=node.get("user", "");
            	staticPass=node.get("password", "");
            }
        } catch (StorageException e1) {
        	e1.printStackTrace();
            staticMethod="No proxy";
        }
	}
	@Execute
	public void execute(Display display) {		
		this.shell=new Shell(SWT.TITLE|SWT.SYSTEM_MODAL| SWT.CLOSE | SWT.MAX);
	    this.shell.setSize(320,380);
	    this.shell.setText("Proxy preferences");
	    GridLayout gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		this.shell.setLayout(gridLayout);
				
		Composite header=new Composite(this.shell, SWT.NONE);
		gridLayout=new GridLayout();
		gridLayout.numColumns=2;
		header.setLayout(gridLayout);
		GridData gridData=new GridData(GridData.FILL_BOTH);
		header.setLayoutData(gridData);
		
		Label typeLabel=new Label(header, SWT.NONE);
		typeLabel.setText("Proxy type");
		gridData=new GridData();
		gridData.widthHint=60;
		typeLabel.setLayoutData(gridData);
		
		this.combo=new Combo(header, SWT.DROP_DOWN | SWT.BORDER | SWT.WRAP);
		this.combo.add("No proxy");
		this.combo.add("Native");
		this.combo.add("Manual");
		
		this.bodyPart=new Composite(this.shell, SWT.NONE);
		gridLayout=new GridLayout();
		gridLayout.numColumns=1;
		this.bodyPart.setLayout(gridLayout);
		gridData=new GridData(GridData.FILL_BOTH);
		this.bodyPart.setLayoutData(gridData);
		
		this.body=new Composite(this.bodyPart, SWT.NONE);
		
		if(staticMethod!=null){
			if(staticMethod.compareTo("Manual")==0){
				this.combo.setText("Manual");
    			body.dispose();
				body=changeBodyToManual();
				GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
				data.horizontalSpan=1;
				data.verticalSpan=1;
				body.setLayoutData(data);		    
				shell.layout(true, true);		
			}else if(staticMethod.compareTo("Native")==0){
				this.combo.setText("Native");
    			body.dispose();
				body=new Composite(bodyPart, SWT.NONE);
				GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
				data.horizontalSpan=1;
				data.verticalSpan=1;
				body.setLayoutData(data);		    
				shell.layout(true, true);		
			}
			else{
				this.combo.setText("No proxy");
    			body.dispose();
				body=new Composite(bodyPart, SWT.NONE);
				GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
				data.horizontalSpan=1;
				data.verticalSpan=1;
				body.setLayoutData(data);		    
				shell.layout(true, true);		
			}
		}
		
		gridData=new GridData();
		gridData.widthHint=150;
		this.combo.setLayoutData(gridData);
		this.combo.addListener(SWT.KeyDown, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		event.doit = false; 
	    	} 
    	}); 
		this.combo.addListener(SWT.Selection, new Listener(){ 
	    	public void handleEvent(Event event) { 
	    		String choice=combo.getText();
	    		if(choice.compareTo("No proxy")==0){
	    			body.dispose();
					body=new Composite(bodyPart, SWT.NONE);
					GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
					data.horizontalSpan=1;
					data.verticalSpan=1;
					body.setLayoutData(data);		    
					shell.layout(true, true);		
	    		}else if(choice.compareTo("Native")==0){
	    			body.dispose();
					body=new Composite(bodyPart, SWT.NONE);
					GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
					data.horizontalSpan=1;
					data.verticalSpan=1;
					body.setLayoutData(data);		    
					shell.layout(true, true);		
	    		}else if(choice.compareTo("Manual")==0){
	    			body.dispose();
					body=changeBodyToManual();
					GridData data = new GridData(SWT.FILL, SWT.NONE, false, false);	    
					data.horizontalSpan=1;
					data.verticalSpan=1;
					body.setLayoutData(data);		    
					shell.layout(true, true);		
	    		}
	    	} 
    	}); 
		
		Composite buttonPart=new Composite(this.shell, SWT.NONE);
		GridLayout gl=new GridLayout();
		gl.numColumns=2;
		gl.horizontalSpacing=10;
		buttonPart.setLayout(gl);
		
		Button ok=new Button(buttonPart, SWT.PUSH);
		ok.setText("OK");
		GridData gd=new GridData();
		gd.widthHint=100;
		ok.setLayoutData(gd);
		ok.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				if(combo.getText().compareTo("No proxy")==0){
					try {
						node.put("method", "No proxy", false);
					} catch (StorageException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					staticMethod="No proxy";
				}else if(combo.getText().compareTo("Native")==0){
					try {
						node.put("method", "Native", false);
					} catch (StorageException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					staticMethod="Native";
				}else if(combo.getText().compareTo("Manual")==0){
					if(host.getText().compareTo("")==0){
						displayMessage("Host is not set");
						return;
					}
					if(port.getText().compareTo("")==0){
						displayMessage("Port is not set");
						return;
					}
					if(authRequired.getSelection()){
						if(user.getText().compareTo("")==0){
							displayMessage("User is not set");
							return;
						}
						if(pass.getText().compareTo("")==0){
							displayMessage("Password is not set");
							return;
						}
					}
					try {
						node.put("method", "Manual", false);
					} catch (StorageException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					staticMethod="Manual";
					try {
			            node.put("host", host.getText(), true);
					 } catch (StorageException e1) {
				            e1.printStackTrace();
				     }
		            staticHost=host.getText();
		            try{
			            node.put("port", port.getText(), true);
		            } catch (StorageException e1) {
			            e1.printStackTrace();
			        }
		            staticPort=port.getText();
		            try{
			            node.putBoolean("authRequired", authRequired.getSelection(), true);
		            } catch (StorageException e1) {
			            e1.printStackTrace();
			        }
		            staticAuthRequired=authRequired.getSelection();
		            if(authRequired.getSelection()){
		            	try{
			            	node.put("user", user.getText(), true);
		            	 } catch (StorageException e1) {
					            e1.printStackTrace();
					     }
		            	staticUser=user.getText();
		            	try{
				            node.put("password", pass.getText(), true);
		            	 } catch (StorageException e1) {
					            e1.printStackTrace();
					     }
			            staticPass=pass.getText();
		            }
				}
				
				shell.close();
			}
		});
        
		Button cancel=new Button(buttonPart, SWT.PUSH);
		cancel.setText("Cancel");
		gd=new GridData();
		gd.widthHint=100;
		cancel.setLayoutData(gd);
		cancel.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				shell.close();
			}
		});
		
		Composite spacer=new Composite(this.shell, SWT.NONE);
		spacer.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		//shell.setSize(shell.computeSize(SWT.DEFAULT, SWT.DEFAULT));
		this.shell.open();
		while(!shell.isDisposed()){
	    	if (!display.readAndDispatch()) {
	            display.sleep();
	          }
	    }
	}
	private Composite changeBodyToManual(){
		Composite manualPart=new Composite(this.bodyPart, SWT.NONE);
		GridLayout gl=new GridLayout();
		gl.numColumns=1;
		gl.horizontalSpacing=0;
		gl.verticalSpacing=0;
		manualPart.setLayout(gl);
		//manualPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Composite serverPart=new Composite(manualPart, SWT.NONE);
		gl=new GridLayout();
		gl.numColumns=2;
		gl.horizontalSpacing=5;
		gl.verticalSpacing=5;
		serverPart.setLayout(gl);
		serverPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Label hostLabel=new Label(serverPart, SWT.NONE);
		hostLabel.setText("Host:");
		GridData gridData=new GridData();
		gridData.widthHint=60;
		hostLabel.setLayoutData(gridData);
		this.host=new Text(serverPart, SWT.BORDER);
		gridData=new GridData();
		gridData.widthHint=150;
		this.host.setLayoutData(gridData);
		if(staticHost!=null && staticHost.compareTo("")!=0){
			this.host.setText(staticHost);
		}
		
		Label portLabel=new Label(serverPart, SWT.NONE);
		portLabel.setText("Port:");
		gridData=new GridData();
		gridData.widthHint=60;
		portLabel.setLayoutData(gridData);
		this.port=new Text(serverPart, SWT.BORDER);
		gridData=new GridData();
		gridData.widthHint=150;
		this.port.setLayoutData(gridData);
		if(staticPort!=null && staticPort.compareTo("")!=0){
			this.port.setText(staticPort);
		}
		
		this.authRequired=new Button(manualPart, SWT.CHECK);
		this.authRequired.setText("Authentification required");
		this.authRequired.addListener(SWT.Selection, new Listener(){
			@Override
			public void handleEvent(Event event) {
				boolean bool=authRequired.getSelection();
				user.setEditable(bool);
				pass.setEditable(bool);
			}
		});
		this.authRequired.setSelection(staticAuthRequired);

		
		Composite authPart=new Composite(manualPart, SWT.NONE);
		gl=new GridLayout();
		gl.numColumns=2;
		gl.horizontalSpacing=5;
		gl.verticalSpacing=5;
		authPart.setLayout(gl);
		authPart.setLayoutData(new GridData(GridData.FILL_BOTH));
		
		Label userLabel=new Label(authPart, SWT.NONE);
		userLabel.setText("User name:");
		gridData=new GridData();
		gridData.widthHint=60;
		userLabel.setLayoutData(gridData);
		this.user=new Text(authPart, SWT.BORDER);
		gridData=new GridData();
		gridData.widthHint=150;
		this.user.setLayoutData(gridData);
		this.user.setEditable(staticAuthRequired);
		if(staticUser!=null && staticUser.compareTo("")!=0){
			this.user.setText(staticUser);
		}
		
		Label passLabel=new Label(authPart, SWT.NONE);
		passLabel.setText("Password:");
		gridData=new GridData();
		gridData.widthHint=60;
		passLabel.setLayoutData(gridData);
		this.pass=new Text(authPart, SWT.BORDER | SWT.PASSWORD);
		gridData=new GridData();
		gridData.widthHint=150;
		this.pass.setLayoutData(gridData);
		this.pass.setEditable(staticAuthRequired);
		if(staticPass!=null && staticPass.compareTo("")!=0){
			this.pass.setText(staticPass);
		}
		
		
		return manualPart;
	}
	public static String getMethod(){
		return staticMethod;
	}
	public static String getHost(){
		return staticHost;
	}
	public static String getPort(){
		return staticPort;
	}
	public static boolean getAuthRequired(){
		return staticAuthRequired;
	}
	public static String getUser(){
		return staticUser;
	}
	public static String getPass(){
		return staticPass;
	}
	public void displayMessage(String message){
	    int style = SWT.ICON_INFORMATION | SWT.OK;
	    MessageBox messageBox = new MessageBox(new Shell(), style);
	    messageBox.setMessage(message);
	    messageBox.open();
	}
}

