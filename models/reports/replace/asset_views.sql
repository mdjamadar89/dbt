{{ config(tags=["every_night"]) }}

SELECT 
        (CASE
            WHEN (internal_links.object:target::VARCHAR = 'twitter') THEN (COUNT(internal_link_clicks.object:id::INT) * 0.05)
            ELSE COUNT(internal_link_clicks.object:id::INT)
        END) AS views,
        internal_links.id AS internal_link_id,
        (CASE
            WHEN
                ((internal_links.object:target::VARCHAR = 'com.microsoft.office.outlook')
                    OR (internal_links.object:target::VARCHAR = 'com.yahoo.mobile.client.android.mail')
                    OR (internal_links.object:target::VARCHAR = 'com.yahoo.mobile.client.android.mail.att')
                    OR (internal_links.object:target::VARCHAR = 'com.easilydo.mail')
                    OR (internal_links.object:target::VARCHAR = 'com.mail.mobile.android.mail')
                    OR (internal_links.object:target::VARCHAR = 'com.outlook.Z7')
                    OR (internal_links.object:target::VARCHAR = 'gmail')
                    OR (internal_links.object:target::VARCHAR = 'outlook')
                    OR (internal_links.object:target::VARCHAR = 'com.microsoft.Office.Outlook.compose-shareextension')
                    OR (internal_links.object:target::VARCHAR = 'yahoo mail')
                    OR (internal_links.object:target::VARCHAR = 'com.aol.mobile.aolapp')
                    OR (internal_links.object:target::VARCHAR = 'com.apple.UIKit.activity.Mail')
                    OR (internal_links.object:target::VARCHAR = 'com.google.Gmail.ShareExtension')
                    OR (internal_links.object:target::VARCHAR = 'hotmail.sign.in.hot.mail.app')
                    OR (internal_links.object:target::VARCHAR LIKE '%email%'))
            THEN
                'email'
            WHEN
                ((internal_links.object:target::VARCHAR = 'com.android.messaging')
                    OR (internal_links.object:target::VARCHAR = 'com.android.mms')
                    OR (internal_links.object:target::VARCHAR = 'com.samsung.android.messaging')
                    OR (internal_links.object:target::VARCHAR = 'com.textra')
                    OR (internal_links.object:target::VARCHAR = 'com.verizon.messaging.vzmsgs')
                    OR (internal_links.object:target::VARCHAR = 'com.blackberry.hub')
                    OR (internal_links.object:target::VARCHAR = 'text_message')
                    OR (internal_links.object:target::VARCHAR = 'com.htc.sense.mms')
                    OR (internal_links.object:target::VARCHAR = 'com.google.android.apps.fireball')
                    OR (internal_links.object:target::VARCHAR = 'com.jb.gosms')
                    OR (internal_links.object:target::VARCHAR = 'com.bbm')
                    OR (internal_links.object:target::VARCHAR = 'message+')
                    OR (internal_links.object:target::VARCHAR = 'messagerie')
                    OR (internal_links.object:target::VARCHAR = 'messages')
                    OR (internal_links.object:target::VARCHAR = 'messaging')
                    OR (internal_links.object:target::VARCHAR = 'messenger')
                    OR (internal_links.object:target::VARCHAR = 'messenger lite')
                    OR (internal_links.object:target::VARCHAR = 'next sms')
                    OR (internal_links.object:target::VARCHAR = 'com.motorola.messaging')
                    OR (internal_links.object:target::VARCHAR = 'com.groupme.android')
                    OR (internal_links.object:target::VARCHAR = 'com.apple.UIKit.activity.Message')
                    OR (internal_links.object:target::VARCHAR = 'com.isaiasmatewos.texpandpro')
                    OR (internal_links.object:target::VARCHAR = 'texpand plus')
                    OR (internal_links.object:target::VARCHAR = 'texpand')
                    OR (internal_links.object:target::VARCHAR = 'com.calea.echo')
                    OR (internal_links.object:target::VARCHAR = 'com.tencent.mm')
                    OR (internal_links.object:target::VARCHAR = 'jp.naver.line.android')
                    OR (internal_links.object:target::VARCHAR = 'xyz.klinker.messenger'))
            THEN
                'sms'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%print%')
            THEN
                'print'
            WHEN
                ((internal_links.object:target::VARCHAR = 'com.alensw.PicFolder')
                    OR (internal_links.object:target::VARCHAR = 'com.cardinalblue.piccollage.google')
                    OR (internal_links.object:target::VARCHAR = 'com.google.android.apps.photos')
                    OR (internal_links.object:target::VARCHAR = 'com.amazon.clouddrive.photos')
                    OR (internal_links.object:target::VARCHAR = 'com.androidvilla.addwatermark.free')
                    OR (internal_links.object:target::VARCHAR = 'com.jb.zcamera')
                    OR (internal_links.object:target::VARCHAR = 'com.lyrebirdstudio.montagenscolagem')
                    OR (internal_links.object:target::VARCHAR = 'com.magicv.airbrush')
                    OR (internal_links.object:target::VARCHAR = 'com.picmonkey.picmonkey')
                    OR (internal_links.object:target::VARCHAR = 'com.picsart.studio')
                    OR (internal_links.object:target::VARCHAR = 'photos')
                    OR (internal_links.object:target::VARCHAR = 'com.toyopagroup.picaboo.share')
                    OR (internal_links.object:target::VARCHAR = 'com.apple.UIKit.activity.SaveToCameraRoll')
                    OR (internal_links.object:target::VARCHAR = 'com.roidapp.photogrid'))
            THEN
                'photo'
            WHEN
                ((internal_links.object:target::VARCHAR = 'com.google.android.keep')
                    OR (internal_links.object:target::VARCHAR = 'com.samsung.android.app.memo')
                    OR (internal_links.object:target::VARCHAR = 'com.socialnmobile.dictapps.notepad.color.note')
                    OR (internal_links.object:target::VARCHAR = 'com.samsung.android.snote')
                    OR (internal_links.object:target::VARCHAR = 'com.evernote.iPhone.Evernote.EvernoteShare')
                    OR (internal_links.object:target::VARCHAR = 'com.microsoft.office.onenote')
                    OR (internal_links.object:target::VARCHAR = 'note')
                    OR (internal_links.object:target::VARCHAR LIKE '%notes%'))
            THEN
                'notes'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%copy%')
            THEN
                'copy'
            WHEN
                ((internal_links.object:target::VARCHAR = 'codeadore.textgram')
                    OR (internal_links.object:target::VARCHAR = 'com.adobe.reader')
                    OR (internal_links.object:target::VARCHAR = 'com.fotoswipe.android')
                    OR (internal_links.object:target::VARCHAR = 'com.myfitnesspal.android')
                    OR (internal_links.object:target::VARCHAR = 'com.samsung.android.app.simplesharing')
                    OR (internal_links.object:target::VARCHAR = 'line')
                    OR (internal_links.object:target::VARCHAR = 'org.buffer.android')
                    OR (internal_links.object:target::VARCHAR = 'org.mozilla.firefox')
                    OR (internal_links.object:target::VARCHAR = 'com.levelup.touiteur'))
            THEN
                'social'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%AirDrop%')
            THEN
                'airdrop'
            WHEN (internal_links.object:target::VARCHAR = 'net.bloomz') THEN 'bloomz'
            WHEN (internal_links.object:target::VARCHAR = 'com.android.bluetooth') THEN 'bluetooth'
            WHEN (internal_links.object:target::VARCHAR = 'com.discord') THEN 'discord'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%Dropbox%')
            THEN
                'dropbox'
            WHEN (internal_links.object:target::VARCHAR = 'com.apple.UIKit.activity.PostToFacebook') THEN 'facebook'
            WHEN
                ((internal_links.object:target::VARCHAR = 'com.facebook.Messenger.ShareExtension')
                    OR (internal_links.object:target::VARCHAR = 'com.facebook.talk.ShareExtension'))
            THEN
                'facebook_messenger'
            WHEN (internal_links.object:target::VARCHAR = 'com.google.Drive.ShareExtension') THEN 'google drive'
            WHEN (internal_links.object:target::VARCHAR = 'com.google.hangouts.ShareExtension') THEN 'google hangouts'
            WHEN (internal_links.object:target::VARCHAR = 'com.google.android.apps.googlevoice') THEN 'google voice'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%hootsuite%')
            THEN
                'hootsuite'
            WHEN (internal_links.object:target::VARCHAR = 'com.hubspot.android') THEN 'hubspot'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%iCloud%')
            THEN
                'iCloud'
            WHEN
                ((internal_links.object:target::VARCHAR = 'com.kimcy929.repost')
                    OR (internal_links.object:target::VARCHAR LIKE '%instagram%'))
            THEN
                'instagram'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%linkedin%')
            THEN
                'linkedin'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%marcopolo%')
            THEN
                'marco polo'
            WHEN (internal_links.object:target::VARCHAR = 'mega.privacy.android.app') THEN 'mega'
            WHEN (internal_links.object:target::VARCHAR = 'com.mix.android') THEN 'mix'
            WHEN (internal_links.object:target::VARCHAR = 'com.nextdoor') THEN 'nextdoor'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%parler%')
            THEN
                'parler'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%pinterest%')
            THEN
                'pinterest'
            WHEN (internal_links.object:target::VARCHAR = 'com.samsung.android.scloud') THEN 'samsung cloud'
            WHEN (internal_links.object:target::VARCHAR LIKE '%skype%') THEN 'skype'
            WHEN (internal_links.object:target::VARCHAR = 'com.samsung.android.oneconnect') THEN 'smart_things'
            WHEN (internal_links.object:target::VARCHAR = 'com.tencent.xin.sharetimeline') THEN 'tencent'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%tumblr%')
            THEN
                'tumblr'
            WHEN (internal_links.object:target::VARCHAR = 'com.truecaller') THEN 'truecaller'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%Twitter%')
            THEN
                'twitter'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%voxer%')
            THEN
                'voxer'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%whatsapp%')
            THEN
                'whatsapp'
            WHEN (internal_links.object:target::VARCHAR = 'com.yelp.android') THEN 'yelp'
            WHEN
                (internal_links.object:target::VARCHAR LIKE '%zoom%')
            THEN
                'zoom'
            ELSE internal_links.object:target::VARCHAR
        END) AS target,
        internal_links.object:slug::VARCHAR AS slug,
        (CASE
            WHEN (internal_links.object:controller::VARCHAR = 'links') THEN 'images'
            WHEN (internal_links.object:controller::VARCHAR = 'panels') THEN 'web_pages'
            ELSE internal_links.object:controller::VARCHAR
        END) AS controller,
        internal_link_clicks.object:created::DATETIME AS link_created,
        internal_links.object:site_id::VARCHAR AS site_id
    FROM
        ((repsites.mysql_parquet_complete.internal_link_clicks
        JOIN repsites.mysql_parquet_complete.internal_links ON ((internal_link_clicks.object:internal_link_id::INT = internal_links.id)
            AND (internal_link_clicks.schema_name = internal_links.schema_name)))
        JOIN repsites.mysql_parquet_complete.users ON ((internal_links.object:user_id::INT = users.id)
            AND (internal_links.schema_name = users.schema_name)))
    GROUP BY internal_links.id, internal_links.object:target::VARCHAR, internal_links.object:slug::VARCHAR, internal_links.object:controller::VARCHAR, internal_link_clicks.object:created::DATETIME, internal_links.object:site_id::VARCHAR, internal_link_clicks.schema_name
    ORDER BY internal_links.object:site_id::VARCHAR, internal_links.id, internal_link_clicks.schema_name