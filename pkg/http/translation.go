package http

import (
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/locales"
	"github.com/go-playground/locales/en"
	"github.com/go-playground/locales/vi"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	entranslations "github.com/go-playground/validator/v10/translations/en"
	vitranslations "github.com/go-playground/validator/v10/translations/vi"
	"github.com/hyuti/API-Golang-Template/pkg/collection"
	"reflect"
	"strings"
	"sync"
)

var (
	ErrValidatorEngineNotFound = errors.New("validator engine not found")
	ErrTranslatorNotInit       = errors.New("translator not init")
	uni                        *ut.UniversalTranslator
	mutex                      sync.Mutex
	locators                   map[string]string
)

func init() {
	mutex.Lock()
	defer mutex.Unlock()
	locators = make(map[string]string)
}

const (
	English = "en"
	Vietnam = "vi"
)

type TranslatorRegistra interface {
	Lang() locales.Translator
	Register(v *validator.Validate, trans ut.Translator) error
}
type Registra func(v *validator.Validate, trans ut.Translator) (err error)

func NewTranslatorRegistra(l locales.Translator, r Registra) TranslatorRegistra {
	return translatorRegistra{
		locale:   l,
		registra: r,
	}
}

func DefaultTranslation() error {
	return WithTranslation(WithEnTranslation(), WithViTranslation())
}

func WithTranslation(translators ...TranslatorRegistra) error {
	mutex.Lock()
	defer mutex.Unlock()
	if uni != nil {
		return nil
	}
	langs := collection.Map[TranslatorRegistra, locales.Translator](translators, func(item TranslatorRegistra, _ int) locales.Translator {
		return item.Lang()
	})
	uni = ut.New(en.New(), langs...)

	e, ok := binding.Validator.Engine().(*validator.Validate)
	if !ok {
		return ErrValidatorEngineNotFound
	}
	e.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("json"), ",", 2)[0]

		if name == "-" {
			return ""
		}

		return name
	})
	return collection.ForEach(translators, func(item TranslatorRegistra, _ int) error {
		trans, ok := uni.GetTranslator(item.Lang().Locale())
		if !ok {
			return nil
		}
		if err := item.Register(e, trans); err != nil {
			return err
		}
		return nil
	})
}

func RegisterLocator(code, locale string) {
	mutex.Lock()
	defer mutex.Unlock()
	locators[code] = locale
}

func WithEnTranslation() TranslatorRegistra {
	RegisterLocator("en-US", English)
	return NewTranslatorRegistra(en.New(), entranslations.RegisterDefaultTranslations)
}

func WithViTranslation() TranslatorRegistra {
	RegisterLocator("vi", Vietnam)
	return NewTranslatorRegistra(vi.New(), vitranslations.RegisterDefaultTranslations)
}

func Translator(locale string) (ut.Translator, error) {
	mutex.Lock()
	defer mutex.Unlock()
	if uni == nil {
		return nil, ErrTranslatorNotInit
	}
	trans, _ := uni.GetTranslator(locale)
	return trans, nil
}

func EnTranslator() (ut.Translator, error) {
	return Translator(English)
}
func ViTranslator() (ut.Translator, error) {
	return Translator(Vietnam)
}

type Header struct {
	AcceptLanguage string `header:"Accept-Language"`
}

func TranslatorCtx(ctx *gin.Context) (ut.Translator, error) {
	var h Header
	if err := ctx.ShouldBindHeader(&h); err != nil {
		return EnTranslator()
	}
	mutex.Lock()
	loc, ok := locators[h.AcceptLanguage]
	mutex.Unlock()
	if !ok {
		return EnTranslator()
	}
	return Translator(loc)
}

type translatorRegistra struct {
	locale   locales.Translator
	registra Registra
}

func (t translatorRegistra) Lang() locales.Translator {
	return t.locale
}

func (t translatorRegistra) Register(v *validator.Validate, trans ut.Translator) error {
	return t.registra(v, trans)
}
