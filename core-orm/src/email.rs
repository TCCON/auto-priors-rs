use std::{path::PathBuf, process::{Command, Stdio}, io::Write};

use lettre::{SmtpTransport, Transport, Message, message::Mailbox, Address};
use serde::{Deserialize, Serialize};

use crate::error::EmailError;

/// A trait that any email backend must implement
pub trait SendMail {
    /// Send an email
    fn send_mail(&self, to: &[&str], from: &str, cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<(), EmailError>;
}

/// A struct used to send emails by calling the `mailx` utility via the shell
/// 
/// The default is to use shell PATH resolution to get the `mail` program. If 
/// that does not work for your system, you can specify an alternate executable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mailx {
    exec: PathBuf
}

impl Mailx {
    /// Create an instance that calls `mailx` at the given path
    pub fn new(exec: PathBuf) -> Self {
        Self { exec }
    }
}

impl Default for Mailx {
    fn default() -> Self {
        Self { exec: PathBuf::from("mail") }
    }
}

impl SendMail for Mailx {
    fn send_mail(&self, to: &[&str], from: &str, cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<(), EmailError> {
        let mut cmd = Command::new(&self.exec);
        cmd.stdin(Stdio::piped())
            .args(["-r", from])
            .args(["-s", subject])
            .arg(to.join(","));

        if let Some(cc) = cc {
            cmd.args(["-c", cc.join(",").as_str()]);
        }

        if let Some(bcc) = bcc {
            cmd.args(["-b", bcc.join(",").as_str()]);
        }

        let mut p = cmd.spawn()
            .map_err(|e| EmailError::SendFailure(format!("Unable to spawn child process to send email: {e}")))?;

        let mut stdin = p.stdin.take()
            .ok_or_else(|| EmailError::SendFailure(format!("Failed to get stdin to mailx process")))?;
        stdin.write_all(message.as_bytes())
            .map_err(|e| EmailError::SendFailure(format!("Failed to write message to mail process stdin: {e}")))?;

        Ok(())
    }
}


/// An enum defining different connection types the Lettre crate can use.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum LettreSmtpType {
    /// Establish an unencrypted connection to an SMTP server running on the local host.
    /// This is the default.
    Local
}

impl Default for LettreSmtpType {
    fn default() -> Self {
        Self::Local
    }
}


/// A struct used to send emails by directly connecting to an SMTP server with the Lettre crate.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Lettre {
    smtp: LettreSmtpType
}

impl Lettre {
    /// Create an instance of this backend with a specified SMTP connection
    pub fn new(smtp: LettreSmtpType) -> Self {
        Self { smtp }
    }
}

impl SendMail for Lettre {
    fn send_mail(&self, to: &[&str], from: &str, cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<(), EmailError> {
        
        // Construct the basis of the email with the from address and subject
        let email = Message::builder()
            .from(parse_email_address(from)?)
            .subject(subject);

        // Add all of the recipients to the email
        let to = to.into_iter()
            .map(|&a| parse_email_address(a))
            .collect::<Result<Vec<_>, _>>()?;
        let email = to.into_iter()
            .fold(email, |e, a| e.to(a));

        // Add any cc'd recipients
        let email = if let Some(cc) = cc {
            let cc = cc.into_iter()
            .map(|c| parse_email_address(c))
            .collect::<Result<Vec<_>, _>>()?;
            
            cc.into_iter()
                .fold(email, |e, c| e.cc(c))
        } else {
            email
        };

        // Add any bcc'd recipients
        let email = if let Some(bcc) = bcc {
            let bcc = bcc.into_iter()
                .map(|&b| parse_email_address(b))
                .collect::<Result<Vec<_>, _>>()?;
            
            bcc.into_iter()
                    .fold(email, |e, b| e.bcc(b))
        } else {
            email
        };

        // Add the body to the message - this converts the MessageBuilder to a 
        // Message so it has to go last.
        let email = email.body(message.to_string())
            .map_err(|e| EmailError::UnencodableBody(e.to_string()))?;

        // Send the email
        let mailer = match self.smtp {
            LettreSmtpType::Local => SmtpTransport::unencrypted_localhost()
        };
        mailer.send(&email)
            .map_err(|e| EmailError::SendFailure(e.to_string()))?;

        Ok(())
    }
}


/// A struct used to send emails by directly connecting to an SMTP server with the Lettre crate.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MockEmail {}

impl SendMail for MockEmail {
    fn send_mail(&self, to: &[&str], from: &str, cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<(), EmailError> {
        use std::fmt::Write;
        let mut msg = "== Mock email ==\n".to_string();
        
        writeln!(&mut msg, "To: {}", to.join(","))
            .map_err(|e| EmailError::SendFailure(e.to_string()))?;
        
        writeln!(&mut msg, "From: {}", from)
            .map_err(|e| EmailError::SendFailure(e.to_string()))?;
        
        if let Some(cc) = cc {
            writeln!(&mut msg, "CC: {}", cc.join(","))
                .map_err(|e| EmailError::SendFailure(e.to_string()))?;
        }

        if let Some(bcc) = bcc {
            writeln!(&mut msg, "BCC: {}", bcc.join(","))
                .map_err(|e| EmailError::SendFailure(e.to_string()))?;
        }

        writeln!(&mut msg, "Subject: {}", subject)
            .map_err(|e| EmailError::SendFailure(e.to_string()))?;

        writeln!(&mut msg, "Body: {}", message)
            .map_err(|e| EmailError::SendFailure(e.to_string()))?;

        write!(&mut msg, "== End mock email ==")
            .map_err(|e| EmailError::SendFailure(e.to_string()))?;

        println!("{}", msg);
        Ok(())
    }
}


/// A convenience function to parse a string into a [`Lettre::Mailbox`] with
/// no name, just an email address.
pub fn parse_email_address(email: &str) -> Result<Mailbox, EmailError> {
    let email = email.parse::<Address>()
        .map_err(|_| EmailError::UnparsableEmail(email.to_string()))?;
    
    Ok(Mailbox::new(None, email))
}