

import datetime
agora = datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")

suporte_email = {

    "dados_sucesso" : {
        "assunto":f"Atualização da Base Fria foi processado em {agora}",
        "destinatario": "",
        "copias":"",
        "corpo_emal":"" #Somente se precisar usar.
    },

    "dados_falha" : {
        "assunto":f"Possui dupliciadade no processo e precisa ser tratado.",
        "destinatario": "",
        "copias":"",
        "corpo_emal":""
    }

    
}

html_sucesso = """<html dir="ltr" xmlns="http://www.w3.org/1999/xhtml" xmlns:o="urn:schemas-microsoft-com:office:office">
        <head>
            <meta charset="UTF-8">
            <meta content="width=device-width, initial-scale=1" name="viewport">
            <meta name="x-apple-disable-message-reformatting">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta content="telephone=no" name="format-detection">
            <title></title>
            <!--[if (mso 16)]>
            <style type="text/css">
            a {text-decoration: none;}
            </style>
            <![endif]-->
            <!--[if gte mso 9]><style>sup { font-size: 100% !important; }</style><![endif]-->
            <!--[if gte mso 9]>
        <noscript>
                <xml>
                <o:OfficeDocumentSettings>
                <o:AllowPNG></o:AllowPNG>
                <o:PixelsPerInch>96</o:PixelsPerInch>
                </o:OfficeDocumentSettings>
                </xml>
            </noscript>
        <![endif]-->
            <!--[if mso]>
        <style type="text/css">
        ul {
        margin: 0 !important;
        }
        ol {
        margin: 0 !important;
        }
        li {
        margin-left: 47px !important;
        }

        </style><![endif]
        -->
        </head>
        <body class="body">
            <div dir="ltr" class="es-wrapper-color">
            <!--[if gte mso 9]>
                    <v:background xmlns:v="urn:schemas-microsoft-com:vml" fill="t">
                        <v:fill type="tile" color="#fafafa"></v:fill>
                    </v:background>
                <![endif]-->
            <table width="100%" cellspacing="0" cellpadding="0" class="es-wrapper">
                <tbody>
                <tr>
                    <td valign="top" class="esd-email-paddings">
                    <table cellpadding="0" cellspacing="0" align="center" class="es-content esd-header-popover">
                        <tbody>
                        <tr>
                            <td align="center" class="esd-stripe esd-synchronizable-module">
                            <table align="center" cellpadding="0" cellspacing="0" width="600" bgcolor="rgba(0, 0, 0, 0)" class="es-content-body" style="background-color:transparent">
                                <tbody>
                                <tr>
                                    <td align="left" class="esd-structure es-p20">
                                    <table cellpadding="0" cellspacing="0" width="100%">
                                        <tbody>
                                        <tr>
                                            <td width="560" align="center" valign="top" class="esd-container-frame">
                                            <table cellpadding="0" cellspacing="0" width="100%">
                                            </table>
                                            </td>
                                        </tr>
                                        </tbody>
                                    </table>
                                    </td>
                                </tr>
                                </tbody>
                            </table>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    <table cellpadding="0" cellspacing="0" align="center" class="es-content">
                        <tbody>
                        <tr>
                            <td align="center" class="esd-stripe">
                            <table bgcolor="#ffffff" align="center" cellpadding="0" cellspacing="0" width="600" class="es-content-body">
                                <tbody>
                                <tr>
                                    <td align="left" class="esd-structure es-p30t es-p30b es-p20r es-p20l">
                                    <table cellpadding="0" cellspacing="0" width="100%">
                                        <tbody>
                                        <tr>
                                            <td width="560" align="center" valign="top" class="esd-container-frame">
                                            <table cellpadding="0" cellspacing="0" width="100%">
                                                <tbody>
                                                <tr>
                                                    <td align="center" class="esd-block-image es-p10t es-p10b" style="font-size:0px">
                                                    <a target="_blank">
                                                        <img src="https://fnmhlsn.stripocdn.email/content/guids/CABINET_67e080d830d87c17802bd9b4fe1c0912/images/55191618237638326.png" alt="" width="100" style="display:block">
                                                    </a>
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td align="center" class="esd-block-text es-p10b">
                                                    <h1 class="es-m-txt-c" style="font-size: 46px; line-height: 100%">
                                                        ALOCAÇÃO COMERCIAL
                                                    </h1>
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td align="center" class="esd-block-text es-p5t es-p5b es-p40r es-p40l es-m-p0r es-m-p0l">
                                                    <p>
                                                        Processo que visa avaliar duplicidade na base finalizou e não encontrou erros.
                                                    </p>
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td align="center" class="esd-block-text es-p10t es-p5b">
                                                    <p>
                                                        ​
                                                    </p>
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td align="left" class="esd-block-text es-p5t es-p5b es-p40r es-p40l es-m-p0r es-m-p0l">
                                                    <p>
                                                        Atenciosamente,
                                                    </p>
                                                    </td>
                                                </tr>
                                                <tr>
                                                    <td align="center" class="esd-block-image" style="font-size: 0">
                                                    <a target="_blank">
                                                        <img src="https://fnmhlsn.stripocdn.email/content/guids/CABINET_8aff6c0e6a4f5e852591d3f9c2ee6651c4befad102e2186cecc442f023f235bc/images/assinatura.png" alt="" width="560" class="adapt-img">
                                                    </a>
                                                    </td>
                                                </tr>
                                                </tbody>
                                            </table>
                                            </td>
                                        </tr>
                                        </tbody>
                                    </table>
                                    </td>
                                </tr>
                                </tbody>
                            </table>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    <table cellpadding="0" cellspacing="0" align="center" class="es-content esd-footer-popover">
                        <tbody>
                        <tr>
                            <td align="center" class="esd-stripe esd-synchronizable-module">
                            <table align="center" cellpadding="0" cellspacing="0" width="600" bgcolor="rgba(0, 0, 0, 0)" class="es-content-body" style="background-color:transparent">
                                <tbody>
                                <tr>
                                    <td align="left" class="esd-structure es-p20">
                                    <table cellpadding="0" cellspacing="0" width="100%">
                                        <tbody>
                                        <tr>
                                            <td width="560" align="center" valign="top" class="esd-container-frame">
                                            <table cellpadding="0" cellspacing="0" width="100%">
                                                
                                            </table>
                                            </td>
                                        </tr>
                                        </tbody>
                                    </table>
                                    </td>
                                </tr>
                                </tbody>
                            </table>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                    </td>
                </tr>
                </tbody>
            </table>
            </div>
        </body>
        </html>"""

html_falha = """<td align="center" class="esd-stripe">
  <table bgcolor="#ffffff" align="center" cellpadding="0" cellspacing="0" width="600" class="es-content-body">
    <tbody>
      <tr>
        <td align="left" class="esd-structure es-p30t es-p10b es-p20r es-p20l">
          <table cellpadding="0" cellspacing="0" width="100%">
            <tbody>
              <tr>
                <td width="560" align="center" valign="top" class="esd-container-frame">
                  <table cellpadding="0" cellspacing="0" width="100%">
                    <tbody>
                      <tr>
                        <td align="center" class="esd-block-image es-p10t es-p10b" style="font-size:0px">
                          <a target="_blank">
                            <img src="https://fnmhlsn.stripocdn.email/content/guids/CABINET_a3448362093fd4087f87ff42df4565c1/images/78501618239341906.png" alt="" width="100" style="display:block">
                          </a>
                        </td>
                      </tr>
                      <tr>
                        <td align="center" class="esd-block-text es-p10b">
                          <h1 class="es-m-txt-c" style="font-size:46px;line-height:100%">
                            ALOCAÇÃO COMERCIAL
                          </h1>
                        </td>
                      </tr>
                      <tr>
                        <td align="center" class="esd-block-text es-p5t es-p5b es-p40r es-p40l es-m-p0r es-m-p0l">
                          <p>
                            Processo da atualização apresentou duplicidade e precisa ser tratado com urgência para nova atualização!
                          </p>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </td>
              </tr>
            </tbody>
          </table>
        </td>
      </tr>
      <tr>
        <td align="left" class="esd-structure es-p10t es-p10b es-p20r es-p20l">
          <table cellpadding="0" cellspacing="0" width="100%">
            <tbody>
              <tr>
                <td align="center" valign="top" width="560" class="esd-container-frame">
                  <table cellpadding="0" cellspacing="0" width="100%" style="border-top:2px dashed #cccccc;border-bottom:2px dashed #cccccc;border-radius:5px;border-collapse:separate;border-left:2px dashed #cccccc;border-right:2px dashed #cccccc">
                    <tbody>
                      <tr>
                        <td align="center" class="esd-block-text">
                          <p>
                            O processo precisa ser tratado pois sem esse ajuste, a alocação seá direcionada para o comercial errado, causando desvios nos números.
                          </p>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </td>
              </tr>
            </tbody>
          </table>
        </td>
      </tr>
      <tr>
        <td align="left" class="esd-structure es-p30b es-p20r es-p20l">
          <table cellpadding="0" cellspacing="0" width="100%">
            <tbody>
              <tr>
                <td width="560" align="center" valign="top" class="esd-container-frame">
                  <table cellspacing="0" width="100%" cellpadding="0" style="border-radius:5px;border-collapse:separate">
                    <tbody>
                      <tr>
                        <td align="center" class="esd-block-image" style="font-size: 0">
                          <a target="_blank">
                            <img src="https://fnmhlsn.stripocdn.email/content/guids/CABINET_ccf6843073989d3452174e843e3716c58b8191e2edffc76579d874448666ce03/images/assinatura.png" alt="" width="560" class="adapt-img">
                          </a>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </td>
              </tr>
            </tbody>
          </table>
        </td>
      </tr>
    </tbody>
  </table>
</td>"""