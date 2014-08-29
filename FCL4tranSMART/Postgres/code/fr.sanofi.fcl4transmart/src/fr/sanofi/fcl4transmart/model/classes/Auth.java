package fr.sanofi.fcl4transmart.model.classes;

import java.net.Authenticator;
import java.net.PasswordAuthentication;

public class Auth extends Authenticator {
	private String password;
	private String user;
	public Auth(String user, String pass){
		this.user=user;
		this.password=pass;
	}
	public PasswordAuthentication getPasswordAuthentication () {
		if(this.user==null || this.password==null) return null;
        return new PasswordAuthentication (this.user, this.password.toCharArray());
    }
}
