package telegram

import (
	"errors"
	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/hyuti/Consumer-Golang-Template/pkg/tool"
	"html/template"
)

const (
	errTitle = "ERROR"
	errEmoji = "128560"
	tpl      = `&#{{.Emo}} [{{.Src}}] {{.Func}}
<b>{{.Title}}</b>
<b>{{.Msg}}</b>
<b>Extra:</b> <pre>{{.Extra}}</pre>`
)

var ErrBotEmpty = errors.New("telegram bot expected not to be empty")

type TeleCfg struct {
	FailSilently bool
	Token        string
	ChatID       int64
	Debug        bool
}

type TeleMsg struct {
	Src   string
	Func  string
	Extra any
	Title string
	Emo   string
	Msg   string
}

func (t TeleMsg) String() string {
	return tool.JSONStringify(&t)
}

type Tele struct {
	bot    *tgbotapi.BotAPI
	cfg    *TeleCfg
	msgTpl *template.Template
}

// New -.
func New(cfg *TeleCfg) (*Tele, error) {
	_bot, err := tgbotapi.NewBotAPI(cfg.Token)
	if err != nil && !cfg.FailSilently {
		return nil, err
	}
	if _bot != nil {
		_bot.Debug = cfg.Debug
	}
	msgTpl, err := template.New("").Parse(tpl)
	if err != nil && !cfg.FailSilently {
		return nil, err
	}
	return &Tele{
		bot:    _bot,
		cfg:    cfg,
		msgTpl: msgTpl,
	}, nil
}

// Send -.
func (t *Tele) Send(text string) error {
	msg := tgbotapi.NewMessage(t.cfg.ChatID, text)
	return t.send(msg)
}

func (t *Tele) send(msg tgbotapi.MessageConfig) error {
	if t.bot == nil {
		return ErrBotEmpty
	}

	if _, err := t.bot.Send(msg); err != nil {
		return err
	}
	return nil
}

// SendWithTeleMsg -.
func (t *Tele) SendWithTeleMsg(msg *TeleMsg) error {
	if t.msgTpl == nil {
		return ErrBotEmpty
	}
	b := tool.GetByteBuffer()
	defer tool.ClearAndPutByteBuffer(b)
	if err := t.msgTpl.Execute(b, msg); err != nil {
		return err
	}
	_msg := tgbotapi.NewMessage(t.cfg.ChatID, b.String())
	_msg.ParseMode = tgbotapi.ModeHTML
	return t.send(_msg)
}
func ErrorTeleMsg() *TeleMsg {
	return &TeleMsg{
		Title: errTitle,
		Emo:   errEmoji,
	}
}

func ErrorMsg(msg, src, f string, extra any) *TeleMsg {
	e := ErrorTeleMsg()
	e.Msg = msg
	e.Src = src
	e.Func = f
	e.Extra = extra
	return e
}
