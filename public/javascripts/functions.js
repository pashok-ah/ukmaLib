$(function() {

    $( document ).ready(function() {

        if($( "#my-rating").data('rating') == '0'){
            $("#delete-rate-button").hide();
        }

        $("#book-big-description-truncate").dotdotdot({
            after: 'a.more',
            callback: dotdotdotCallback
        });
        $("#book-big-description-truncate").on('click', 'a', function () {
            if ($(this).text() == "More") {
                var div = $(this).closest('#book-big-description-truncate');
                div.trigger('destroy').find('a.more').hide();
                div.css('max-height', '');
                div.css('height', 'auto');
                $("a.less", div).show();
            } else {
                $(this).closest('#book-big-description-truncate').css("max-height", "50px").dotdotdot({
                    after: "a",
                    callback: dotdotdotCallback
                });
            }
        });

        function dotdotdotCallback(isTruncated, originalContent) {
            if (!isTruncated) {
                $("a", this).remove();
            }
        }


        $(".truncate-description").dotdotdot({

        });
        $(".description-slider").dotdotdot({

        });
    });

	$('.field, textarea').focus(function() {
        if(this.title==this.value) {
            this.value = '';
        }
    }).blur(function(){
        if(this.value=='') {
            this.value = this.title;
        }
    });

    $('#slider ul').jcarousel({
    	scroll: 1,
		auto: 7,
		itemFirstInCallback : mycarousel_firstCallback,
        wrap: 'both'
    });
   function mycarousel_firstCallback(carousel, item, idx) {
        $('#slider .nav a').bind('click', function() {
            carousel.scroll(jQuery.jcarousel.intval($(this).text()));
            $('#slider .nav a').removeClass('active');
            $(this).addClass('active');
            return false;
        });
        $('#slider .nav a').removeClass('active');
        $('#slider .nav a').eq(idx-1).addClass('active');
    }

    $('#book-list ul').jcarousel({
        auto: 5,
        scroll: 1,
        wrap: 'both'
    });



    $("#global-rating").starRating({
        totalStars: 10,
        starSize: 18,
        strokeWidth: 2,
        strokeColor: 'black',
        emptyColor: 'lightgray',
        hoverColor: 'orange',
        activeColor: 'gold',
        useGradient: true,
        readOnly:true,
        starGradient: {
            start: '#000926',
            end: '#004D91'
        }});

    var defaultForMyRating = {
        totalStars: 10,
        starSize: 32,
        strokeWidth: 2,
        strokeColor: 'black',
        emptyColor: 'lightgray',
        hoverColor: 'orange',
        activeColor: 'gold',
        useGradient: false,
        readOnly: false,
        disableAfterRate:false,
        starGradient: {
            start: '#DE4A6C',
            end: '#FFAE28'
        },
        callback: function(currentRating, $el) {
            var bookId = $el.data('book-id');
            var myRatingId = $el.data('myrating-objectid');
            var ajaxUrl = jsRoutes.controllers.BookPageController.saveTheRateAjaxCall(bookId, currentRating);
            if(myRatingId != '0'){
                ajaxUrl = jsRoutes.controllers.BookPageController.updateTheRateAjaxCall(myRatingId, currentRating);
            }
            $.ajax({url: ajaxUrl.url, type: ajaxUrl.type,
                    datatype: "text",
                    success: function(response){
                        switch (response) {
                            case 'Success':
                                onSuccessUpdate(currentRating);
                                break;
                            default:
                                $( "#my-rating").data('myrating-objectid',response);
                                $( "#my-rating").attr('data-myrating-objectid',response);
                                onSuccessUpdate(currentRating);
                                break;
                        }
                    },
                    error: onErrorUpdate});
            console.log('DOM element ', $el);
        }
    };

    var myStarRating = $("#my-rating").starRating(defaultForMyRating);

    $( "#delete-rate-button" ).click(function() {
        var ratingToDelete = $( "#my-rating").data('myrating-objectid');
        var ajaxUrl = jsRoutes.controllers.BookPageController.deleteTheRateAjaxCall(ratingToDelete);
        $.ajax({url: ajaxUrl.url, type: ajaxUrl.type, success: onSuccessDelete});
    });

    var onSuccessUpdate = function(rate) {
        $("#user-rate-info").text('Your rate: ' + rate);
        $("#delete-rate-button").show();
    };

    var onErrorUpdate = function(){
        $("#user-rate-info").text('Error occured!');
    }

    var  onSuccessDelete = function() {
        $("#my-rating").data('plugin_starRating').applyRating(0);
        $( "#my-rating").data('myrating-objectid','0');
        $( "#my-rating").attr('data-myrating-objectid',0);
        $("#user-rate-info").text("Rate this book:");
        $("#delete-rate-button").hide();
    };

    var input = $('input#searchValue');
    var divInput = $('div.input');
    var width = divInput.width();
    var outerWidth = divInput.parent().width() - (divInput.outerWidth() - width) - 28;
    var submit = $('#searchSubmit');
    var txt = input.val();
    input.bind('focus', function() {
        if(input.val() === txt) {
            input.val('');
        }
        $(this).animate({color: '#000'}, 300); // text color
        $(this).parent().animate({
            width: outerWidth + 'px',
            backgroundColor: '#fff', // background color
            paddingRight: '43px'
        }, 300, function() {
            if(!(input.val() === '' || input.val() === txt)) {
                    submit.css({display: 'block'});
            }
        }).addClass('focus');
    }).bind('blur', function() {
        $(this).animate({color: '#b4bdc4'}, 300); // text color
        $(this).parent().animate({
            width: width + 'px',
            backgroundColor: '#e8edf1', // background color
            paddingRight: '15px'
        }, 300, function() {
            if(input.val() === '') {
                input.val(txt)
            }
        }).removeClass('focus');
        if(!($.browser.msie && $.browser.version < 9)) {
            submit.fadeOut(100);
        } else {
            submit.css({display: 'none'});
        }
    }).keyup(function() {
        if(input.val() === '') {
                submit.css({display: 'none'});
        } else {
                submit.css({display: 'block'});
        }
    });
/*     if ($.browser.msie && $.browser.version.substr(0,1)<7) {
        DD_belatedPNG.fix('#logo h1 a, .read-more-btn, #slider .image img, #book-list .jcarousel-prev, #best-sellers .jcarousel-next, #slider .jcarousel-container, #best-sellers .price, .shell, #footer, .products ul li a:hover');
    }*/
});

