use std::{cell::RefCell, collections::VecDeque, fmt::Debug, io::Write, path::PathBuf, process::{Command, Stdio}, sync::{Arc, Mutex}};

use chrono::NaiveDate;
use lettre::{SmtpTransport, Transport, Message, message::Mailbox, Address};
use serde::{Deserialize, Serialize};

use crate::{error::EmailError, MySqlConn, config::Config};

/// A trait that any email backend must implement
pub trait SendMail: Debug {
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
        let email = build_message(to, from, cc, bcc, subject, message)?;

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

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TestingEmail {
    #[serde(skip, default)]
    messages: Arc<Mutex<RefCell<VecDeque<Result<Message, String>>>>>
}

impl TestingEmail {
    pub fn pop_front(&self) -> Option<Result<Message, String>> {
        let mut lock = self.messages.lock()
            .expect("Could not acquire mutex lock");
        let r = lock.get_mut();
        r.pop_front()
    }

    pub fn clear(&self) {
        let mut lock = self.messages.lock()
            .expect("Could not acquire mutex lock");
        let r = lock.get_mut();
        r.clear();
    }

    pub fn num_messages(&self) -> usize {
        let mut lock = self.messages.lock()
            .expect("Could not acquire mutex lock");
        let r = lock.get_mut();
        r.len()
    }
}

impl SendMail for TestingEmail {
    fn send_mail(&self, to: &[&str], from: &str, cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<(), EmailError> {
        let msg_res = build_message(to, from, cc, bcc, subject, message)
            .map_err(|e| e.to_string());

        let mut lock = self.messages.lock()
            .map_err(|e| EmailError::SendFailure(format!("Could not acquire mutex lock: {e}")))?;
        let r = lock.get_mut();
        r.push_back(msg_res);

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

fn build_message(to: &[&str], from: &str, cc: Option<&[&str]>, bcc: Option<&[&str]>, subject: &str, message: &str) -> Result<Message, EmailError> {
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
    email.body(message.to_string())
        .map_err(|e| EmailError::UnencodableBody(e.to_string()))
}


pub async fn email_current_jobs(conn: &mut MySqlConn, config: &Config, to: &[&str]) -> anyhow::Result<()> {
    let summary = crate::jobs::Job::summarize_active_jobs_by_submitter(conn).await?;
    let total = summary.total_num_jobs();
    let subject = "AutoModRust current job summary";
    let now = chrono::Local::now();
    let table = summary.to_table();
    let body = format!("As of {now}, there are {total} priors jobs running or pending:\n\n{table}");

    config.email.send_mail(to, None, None, subject, &body)?;
    Ok(())
}

pub async fn email_completed_jobs(conn: &mut MySqlConn, config: &Config, to: &[&str], start_date: NaiveDate, end_date: Option<NaiveDate>) -> anyhow::Result<()> {
    let (successes, failures) = crate::jobs::Job::summarize_jobs_completed_between(conn, start_date, end_date).await?;
    let (body_intro, subject) = if let Some(end) = end_date {
        let intro = format!("Priors jobs summary for {start_date} to {end}:");
        let subj = format!("AutoModRust completed job summary {start_date} to {end}");
        (intro, subj)
    } else {
        let intro = format!("Priors job summary from {start_date} through now:");
        let subj = format!("AutoModRust completed job summary {start_date} to now");
        (intro, subj)
    };

    let success_body = if successes.is_empty() {
        "No jobs completed during this time period".to_string()
    } else {
        let table = successes.to_table();
        let n = successes.total_num_jobs();
        format!("{n} jobs were completed during this time period:\n\n{table}")
    };

    let failure_body = if failures.is_empty() {
        "No jobs failed during this time period".to_string()
    } else {
        let table = failures.to_table();
        let n = failures.total_num_jobs();
        format!("{n} jobs failed during this time period:\n\n{table}")
    };

    let full_body = format!("{body_intro}\n\n{success_body}\n\n{failure_body}");

    config.email.send_mail(to, None, None, &subject, &full_body)?;
    Ok(())
}