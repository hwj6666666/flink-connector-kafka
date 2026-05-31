# Makefile for SJTUThesis
# 编译产物（.aux / .bbl / .xdv 等）保存在本目录；仅将最终 main.pdf 复制到上级 docs/

THESIS = main
PDF_DIST = ../$(THESIS).pdf

# Option for latexmk
# -g：每轮至少执行一遍规则，避免「Nothing to do」却仍沿用上次失败退出码、且不检测源文件变更的卡住状态
LATEXMK_OPT = -g -time -file-line-error -halt-on-error -interaction=nonstopmode
LATEXMK_OPT_PVC = $(LATEXMK_OPT) -pvc

# make deletion work on Windows
ifdef SystemRoot
	RM = del /Q
	OPEN = start
else
	RM = rm -f
	OPEN = open
endif

.PHONY : all pvc view wordcount clean cleanall FORCE_MAKE

all : $(PDF_DIST)

# 先在本目录生成 PDF，再复制一份到 docs/（不把辅助文件散到 docs/）
$(PDF_DIST) : $(THESIS).tex FORCE_MAKE
	@latexmk $(LATEXMK_OPT) $<
	@cp -f $(THESIS).pdf $(PDF_DIST)
	@echo ">>> 已复制最终 PDF 到 $(PDF_DIST)"

pvc : $(THESIS).tex
	@latexmk $(LATEXMK_OPT_PVC) $(THESIS)

view : $(THESIS).pdf
	$(OPEN) $<

wordcount : $(THESIS).tex
	@if grep -v ^% $< | grep -q '\\documentclass\[[^\[]*lang\s*=\s*en'; then \
		texcount $< -inc -char-only | awk '/total/ {getline; print "英文字符数\t\t\t:",$$4}'; \
	else \
		texcount $< -inc -ch-only   | awk '/total/ {getline; print "纯中文字数\t\t\t:",$$4}'; \
	fi
	@texcount $< -inc -chinese | awk '/total/ {getline; print "总字数（英文单词 + 中文字）\t:",$$4}'

clean :
	-@latexmk -c -silent $(THESIS).tex 2> /dev/null

cleanall :
	-@latexmk -C -silent $(THESIS).tex 2> /dev/null

FORCE_MAKE:
