import yagmail

def sendTickEmails(sendr, rcvr, subj, df):

    yag = yagmail.SMTP(sendr)
    yag.send(
        to=rcvr,
        subject=subj,
        contents=["\n", df.style.render().replace("\n", "")]
    )

    print(f"Sent email successfully to {rcvr}")